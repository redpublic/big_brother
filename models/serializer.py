import json
import dataclasses


class ExtendedJsonSerializer(json.JSONEncoder):
    """
    Extend default json serializer with dataclasses support
    """
    def default(self, obj):
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            return dataclasses.asdict(obj)
        return super().default(obj)
