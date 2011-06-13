from .properties import *


def declare(clazz, **kwargs):
    def build_corn(corn_definition):
        corn = clazz(corn_definition.__name__, **kwargs)
        # Contains
        wrappers = {}

        def append_wrapper(prop):
            if prop._wrapper:
                wrappers[prop._wrapper] = max(prop.depth,
                        wrappers.get(prop._wrapper, 0))
                append_wrapper(prop.wrapped_property)

        def find_prop(prop, wrapper_cls):
            if prop._wrapper == wrapper_cls:
                return prop
            if prop.wrapped_property:
                return find_prop(prop.wrapped_property, wrapper_cls)
        props = {}
        for name in dir(corn_definition):
            prop = getattr(corn_definition, name)
            if isinstance(prop, Property):
                props[name] = prop
                append_wrapper(prop)
                corn.register(name, **(find_prop(prop, None).kwargs))
        for wrapper in sorted(wrappers.items(), key=lambda x: x[1]):
            corn = wrapper(corn)
            for name, prop in props.items():
                wrapped_prop = find_prop(prop, wrapper)
                if wrapped_prop:
                    corn.register(name, **wrapped_prop.kwargs)
        return corn
    return build_corn
