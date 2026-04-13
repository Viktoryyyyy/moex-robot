from __future__ import annotations


class StrategyRegistrationError(Exception):
    pass


class StrategyIdMismatchError(StrategyRegistrationError):
    pass


class ManifestValidationError(StrategyRegistrationError):
    pass


class ConfigValidationError(StrategyRegistrationError):
    pass


class InterfaceValidationError(StrategyRegistrationError):
    pass


class ArtifactContractValidationError(StrategyRegistrationError):
    pass


class UnsupportedModeError(StrategyRegistrationError):
    pass


class ForbiddenResponsibilityError(StrategyRegistrationError):
    pass
