#
# Copyright 2024 Tabs Data Inc.
#

import logging
from enum import Enum
from typing import Dict

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Supported features
class Feature(Enum):
    ENTERPRISE = "enterprise"


# Manage all available features according to accessible packages.
class FeaturesManager:
    def __init__(self) -> None:
        """Initialize the Feature Manager."""
        self._features: Dict[Feature, bool] = {}
        logger.debug("FeaturesManager instantiated")

    @classmethod
    def instance(cls) -> "FeaturesManager":
        """Return the singleton of the FeatureManager."""
        return instance

    @staticmethod
    def validate_feature(feature: Feature) -> bool:
        """Validate that a feature is an actual Feature."""
        return isinstance(feature, Feature)

    def enable(self, feature: Feature):
        """Enable a feature."""
        if self.validate_feature(feature):
            self._features[feature] = True
            logger.debug(f"Feature '{feature.value}' enabled.")
        else:
            logger.debug(f"Feature '{feature}' not enabled: unrecognized.")

    def disable(self, feature: Feature):
        """Disable a feature."""
        if self.validate_feature(feature):
            self._features[feature] = False
            logger.debug(f"Feature '{feature.value}' disabled.")
        else:
            logger.debug(f"Feature '{feature}' not disabled: unrecognized.")

    def is_enabled(self, feature: Feature) -> bool:
        if self.validate_feature(feature):
            return self._features.get(feature, False)
        else:
            return False

    def is_disabled(self, feature: Feature) -> bool:
        """Check if a feature is disabled."""
        return not self.is_enabled(feature)


instance = FeaturesManager()
