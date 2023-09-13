from dataclasses import dataclass
from types import MethodType
from typing import Any, Callable

from tango import server, Database, GreenMode, DevState, AttrWriteType
from tango import DbDevInfo
from tango import GreenMode, DevState, AttrWriteType

from fastcs.attributes import AttrR, AttrRW, AttrW
from fastcs.datatypes import Bool, DataType, Float, Int
from fastcs.exceptions import FastCSException
from fastcs.mapping import Mapping
from fastcs.backend import (
    _link_single_controller_put_tasks,
    _link_attribute_sender_class,
)


@dataclass
class TangoDSROptions:
    dev_name: str = "MY/DEVICE/NAME"
    dev_class: str = "FAST_CS_DEVICE"
    dsr_instance: str = "MY_SERVER_INSTANCE"
    debug: bool = False


def _get_dtype_args(datatype: DataType) -> dict:
    match datatype:
        case Bool(znam, onam):
            return {"dtype": bool}
        case Int():
            return {"dtype": int}
        case Float(prec):
            return {"dtype": float, "format": f"%.{prec}"}
        case _:
            raise FastCSException(f"Unsupported type {type(datatype)}: {datatype}")


def _pick_updater_fget(attr_name, attribute, controller):
    async def fget(self):
        await attribute.updater.update(controller, attribute)
        self.info_stream(f"called fget method: {attr_name}")
        return attribute.get()

    return fget


def _pick_updater_fset(attr_name, attribute, controller):
    async def fset(self, val):
        await attribute.updater.put(controller, attribute, val)
        self.info_stream(f"called fset method: {attr_name}")

    return fset


def _collect_dev_attributes(mapping: Mapping) -> dict:
    collection = {}
    for s_map in mapping.get_controller_mappings():
        path = s_map.controller.path

        for attr_name, attribute in s_map.attributes.items():
            instance = {"label": attr_name}
            instance.update(_get_dtype_args(attribute.datatype))

            polling_period = int(attribute.updater.update_period * 1e3)  # ms
            f_args = (attr_name, attribute, s_map.controller)

            match attribute:
                case AttrRW():
                    instance["fget"] = _pick_updater_fget(*f_args)
                    instance["fset"] = _pick_updater_fset(*f_args)
                    instance["access"] = AttrWriteType.READ_WRITE
                    instance["polling_period"] = polling_period
                case AttrR():
                    # instance["fget"] = lambda *args: 1  # Read one/True
                    instance["fget"] = _pick_updater_fget(*f_args)
                    instance["access"] = AttrWriteType.READ
                    instance["polling_period"] = polling_period
                case AttrW():
                    # instance["fset"] = lambda *args: None  # Do nothing
                    instance["fset"] = _pick_updater_fset(*f_args)
                    instance["access"] = AttrWriteType.WRITE
                    instance["polling_period"] = polling_period

            attr_name = attr_name.title().replace("_", "")
            dev_attr_name = path.upper() + "_" + attr_name if path else attr_name
            collection[dev_attr_name] = server.attribute(**instance)

    return collection


def _pick_command_f(method_name, method, controller):
    async def _dynamic_f(self):
        self.info_stream(f"called {controller} f method: {method_name}")
        return await MethodType(method, controller)()

    _dynamic_f.__name__ = method_name
    return _dynamic_f


def _collect_dev_commands(mapping: Mapping) -> dict:
    collection = {}
    for s_map in mapping.get_controller_mappings():
        path = s_map.controller.path

        for name, method in s_map.command_methods.items():
            instance = {}
            cmd_name = name.title().replace("_", "")
            dev_cmd_name = path.upper() + "_" + cmd_name if path else cmd_name
            f_args = (dev_cmd_name, method.fn, s_map.controller)
            instance["f"] = _pick_command_f(*f_args)
            # instance["dtype_out"] = str  # Read return string for debug
            collection[dev_cmd_name] = server.command(**instance)

    return collection


def _collect_dev_init(mapping: Mapping) -> dict:
    async def init_device(self):
        await server.Device.init_device(self)
        self.set_state(DevState.ON)
        await mapping.controller.connect()

    return {"init_device": init_device}


def _collect_dev_helpers(mapping: Mapping) -> dict:
    collection = {}

    collection["green_mode"] = GreenMode.Asyncio

    return collection


def _collect_dsr_args(debug):
    args = []

    if debug:
        args.append("-v4")

    return args


class TangoDSR:
    def __init__(self, mapping: Mapping):
        self._mapping = mapping

    def _link_process_tasks(self):
        for single_mapping in self._mapping.get_controller_mappings():
            _link_single_controller_put_tasks(single_mapping)
            _link_attribute_sender_class(single_mapping)

    def run(self, options: TangoDSROptions | None = None) -> None:
        if options is None:
            options = TangoDSROptions()

        self._link_process_tasks()

        dev_attributes = _collect_dev_attributes(self._mapping)
        dev_commands = _collect_dev_commands(self._mapping)
        dev_properties = {}
        dev_init = _collect_dev_init(self._mapping)
        dev_helpers = _collect_dev_helpers(self._mapping)

        class_body = {
            **dev_attributes,
            **dev_commands,
            **dev_properties,
            **dev_init,
            **dev_helpers,
        }

        pytango_class = type(options.dev_class, (server.Device,), class_body)
        register_dev(options.dev_name, options.dev_class, options.dsr_instance)

        dsr_args = _collect_dsr_args(options.debug)

        server.run(
            (pytango_class,),
            [options.dev_class, options.dsr_instance, *dsr_args],
            )


def register_dev(dev_name, dev_class, dsr_instance):
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
