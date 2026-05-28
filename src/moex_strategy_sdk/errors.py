class StrategySDKError(Exception):
    """Base error for Strategy SDK contract violations."""


class ManifestValidationError(StrategySDKError):
    """Raised when a strategy manifest violates the SDK contract."""


class ArtifactContractError(StrategySDKError):
    """Raised when an artifact contract is incomplete or invalid."""


class LifecycleError(StrategySDKError):
    """Raised when a lifecycle transition violates the SDK contract."""
