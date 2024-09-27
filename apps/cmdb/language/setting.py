class SettingLanguage:
    def __init__(self, language: str):
        self.language_dict = self.get_language_dict(language)

    def get_language_dict(self, language: str):
        if language == "zh":
            from apps.cmdb.language.languages.zh import LANGUAGE_DICT
        elif language == "en":
            from apps.cmdb.language.languages.en import LANGUAGE_DICT
        else:
            raise Exception("Language not supported")
        return LANGUAGE_DICT

    def get_val(self, _type: str, key: str):
        return self.language_dict.get(_type, {}).get(key)
