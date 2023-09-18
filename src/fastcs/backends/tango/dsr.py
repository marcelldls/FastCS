from dataclasses import dataclass
from types import MethodType
from typing import Any, List, Awaitable, Callable

import tango
from tango import server, AttrWriteType, Database, DbDevInfo, DevState

from fastcs.attributes import AttrR, AttrRW, AttrW
from fastcs.backend import (
    _link_attribute_sender_class,
    _link_single_controller_put_tasks,
)
from fastcs.controller import BaseController
from fastcs.datatypes import Bool, DataType, Float, Int
from fastcs.exceptions import FastCSException
from fastcs.mapping import Mapping


@dataclass
class TangoDSROptions:
    dev_name: str = "MY/DEVICE/NAME"
    dev_class: str = "FAST_CS_DEVICE"
    dsr_instance: str = "MY_SERVER_INSTANCE"
    debug: bool = False


def _get_dtype_args(datatype: DataType) -> dict[str, Any]:
    match datatype:
        case Bool():
            return {"dtype": bool}
        case Int():
            return {"dtype": int}
        case Float(prec):
            return {"dtype": float, "format": f"%.{prec}"}
        case _:
            msg = f"Unsupported type {type(datatype)}: {datatype}"
            raise FastCSException(msg)


def _pick_updater_fget(
    attr_name: str, attribute: AttrR, controller: BaseController
) -> Awaitable[Any]:
    async def fget(self):
        await attribute.updater.update(controller, attribute)
        self.info_stream(f"called fget method: {attr_name}")
        return attribute.get()

    return fget


def _pick_updater_fset(
    attr_name: str, attribute: AttrW, controller: BaseController
) -> Awaitable[None]:
    async def fset(self, val):
        await attribute.updater.put(controller, attribute, val)
        self.info_stream(f"called fset method: {attr_name}")

    return fset


def _collect_dev_attributes(mapping: Mapping) -> dict[str, Any]:
    collection = {}
    for single_mapping in mapping.get_controller_mappings():
        path = single_mapping.controller.path

        for attr_name, attribute in single_mapping.attributes.items():
            attr_name = attr_name.title().replace("_", "")
            d_attr_name = f"{path.upper()}_{attr_name}" if path else attr_name

            instance = {"label": d_attr_name}
            instance.update(_get_dtype_args(attribute.datatype))

            match attribute:
                case AttrRW():
                    instance["fget"] = _pick_updater_fget(
                        attr_name, attribute, single_mapping.controller
                    )
                    instance["fset"] = _pick_updater_fset(
                        attr_name, attribute, single_mapping.controller
                    )
                    instance["access"] = AttrWriteType.READ_WRITE
                    if attribute.updater is not None:
                        polling_period = int(attribute.updater.update_period)
                        instance["polling_period"] = polling_period * 1000
                case AttrR():
                    # instance["fget"] = lambda *args: 1  # Read one/True
                    instance["fget"] = _pick_updater_fget(
                        attr_name, attribute, single_mapping.controller
                    )
                    instance["access"] = AttrWriteType.READ
                    if attribute.updater is not None:
                        polling_period = int(attribute.updater.update_period)
                        instance["polling_period"] = polling_period * 1000
                case AttrW():
                    # instance["fset"] = lambda *args: None  # Do nothing
                    instance["fset"] = _pick_updater_fset(
                        attr_name, attribute, single_mapping.controller
                    )
                    instance["access"] = AttrWriteType.WRITE

            collection[d_attr_name] = server.attribute(**instance)

    return collection


def _pick_command_f(
    method_name: str, method: Callable, controller: BaseController
) -> Callable[..., Awaitable[None]]:
    async def _dynamic_f(self) -> None:
        self.info_stream(f"called {controller} f method: {method_name}")
        return await MethodType(method, controller)()

    _dynamic_f.__name__ = method_name
    return _dynamic_f


def _collect_dev_commands(mapping: Mapping) -> dict[str, Any]:
    collection = {}
    for single_mapping in mapping.get_controller_mappings():
        path = single_mapping.controller.path

        for name, method in single_mapping.command_methods.items():
            instance = {}
            cmd_name = name.title().replace("_", "")
            d_cmd_name = path.upper() + "_" + cmd_name if path else cmd_name
            instance["f"] = _pick_command_f(
                d_cmd_name, method.fn, single_mapping.controller
            )
            # instance["dtype_out"] = str  # Read return string for debug
            collection[d_cmd_name] = server.command(**instance)

    return collection


def _collect_dev_init(mapping: Mapping) -> dict[str, Callable]:
    async def init_device(self):
        await server.Device.init_device(self)
        self.set_state(DevState.ON)
        await mapping.controller.connect()

    return {"init_device": init_device}


def _collect_dev_helpers(mapping: Mapping) -> dict[str, Any]:
    collection = {}

    collection["green_mode"] = tango.GreenMode.Asyncio

    return collection


def _collect_dsr_args(options: TangoDSROptions) -> List:
    args = []

    if options.debug:
        args.append("-v4")

    return args


class TangoDSR:
    def __init__(self, mapping: Mapping):
        self._mapping = mapping

    def _link_process_tasks(self) -> None:
        for single_mapping in self._mapping.get_controller_mappings():
            _link_single_controller_put_tasks(single_mapping)
            _link_attribute_sender_class(single_mapping)

    def run(self, options: TangoDSROptions | None = None) -> None:
        if options is None:
            options = TangoDSROptions()

        self._link_process_tasks()

        dev_attributes = _collect_dev_attributes(self._mapping)
        dev_commands = _collect_dev_commands(self._mapping)
        dev_properties: dict = {}
        dev_init = _collect_dev_init(self._mapping)
        dev_helpers = _collect_dev_helpers(self._mapping)

        class_body = {
            **dev_attributes,
            **dev_commands,
            **dev_properties,
            **dev_init,
            **dev_helpers,
        }

        class_bases = (server.Device,)
        pytango_class = type(options.dev_class, class_bases, class_body)
        register_dev(options.dev_name, options.dev_class, options.dsr_instance)

        dsr_args = _collect_dsr_args(options)

        server.run(
            (pytango_class,),
            [options.dev_class, options.dsr_instance, *dsr_args],
        )


def register_dev(dev_name: str, dev_class: str, dsr_instance: str) -> None:
    dsr_name = f"{dev_class}/{dsr_instance}"
    dev_info = DbDevInfo()
    dev_info.name = dev_name
    dev_info._class = dev_class
    dev_info.server = dsr_name

    db = Database()
    db.delete_device(dev_name)  # Remove existing device if any
    db.add_device(dev_info)

    read_dev_info = db.get_device_info(dev_info.name)

    print("Registered on Tango Database:")
    print(f" - Device: {read_dev_info.name}")
    print(f" - Class: {read_dev_info.class_name}")
    print(f" - Device server: {read_dev_info.ds_full_name}")
