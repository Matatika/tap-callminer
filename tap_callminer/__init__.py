"""Tap for CallMiner."""

from enum import Enum


class CallMinerAPIRegion(str, Enum):
    """Enum class representing available CallMiner API regions."""

    US = ""  # api.callminer.net, idp.callminer.net
    US_FISMA = "f"  # apif.callminer.net, idpf.callminer.net
    UK = "uk"  # apiuk.callminer.net, idpuk.callminer.net
    AU = "aus"  # apiaus.callminer.net, idpaus.callminer.net
    CA = "ca"  # apica.callminer.net, idpca.callminer.net
    EU = "ew"  # apiew.callminer.net, idpew.callminer.net


class CallMinerSearchMode(str, Enum):
    """Enum class representing available CallMiner export search modes."""

    CLIENT_CAPTURE_DATE = "ClientCaptureDate"
    CREATE_DATE = "CreateDate"
    UPDATED = "Updated"
    NEW_AND_UPDATED = "NewAndUpdated"
