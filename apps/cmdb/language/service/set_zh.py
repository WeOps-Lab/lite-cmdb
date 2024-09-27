from apps.cmdb.language.languages.zh import LANGUAGE_DICT


def set_en_language(_type: str, key: str):
    return LANGUAGE_DICT.get(_type, {}).get(key)
