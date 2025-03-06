from __future__ import annotations

from configparser import ConfigParser
from os import PathLike
from collections.abc import Iterator, Mapping



def _readonly(
    self,
    *args,
    **kwargs
) -> None:
    raise RuntimeError("Cannot modify ReadOnlyDict")



class Config:

    filename: str | PathLike = 'config.ini'

    _parser: ConfigParser = ConfigParser()
    _loaded: bool = False


    def __new__(
        cls,
        *args,
        **kwargs
    ) -> Config:

        return cls


    def __class_getitem__(
        cls,
        key: str
    ) -> ConfigSection:

        if not cls._loaded:
            cls._parser.read(cls.filename)
            cls._loaded = True

        return ConfigSection(cls._parser, key)



class ConfigSection(Mapping[str, str]):

    _parser: ConfigParser
    _section: str


    def __init__(
        self,
        parser: ConfigParser,
        key: str
    ) -> ConfigSection:

        self._parser = parser
        self._section = key


    def __getitem__(
        self,
        key: str
    ) -> str:

        return self._parser[self._section][key]


    def __iter__(
        self
    ) -> Iterator[str]:

        return iter(self._parser[self._section])


    def __len__(
        self
    ) -> int:

        return len(self._parser[self._section])



class ReadOnlyDict[K, V](dict):

    __getitem__: V
    __setitem__ = _readonly
    __delitem__ = _readonly
    pop = _readonly
    popitem = _readonly
    clear = _readonly
    update = _readonly
    setdefault = _readonly