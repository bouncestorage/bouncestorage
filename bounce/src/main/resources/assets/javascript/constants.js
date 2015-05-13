var bounceConstants = {};
bounceConstants.awsRegions = [
    { name: "US Standard",
      value: "us-east-1"
    },
    { name: "US West (California)",
      value: "us-west-1"
    },
    { name: "US West (Oregon)",
      value: "us-west-2"
    },
    { name: "EU (Ireland)",
      value: "eu-west-1"
    },
    { name: "Singapore",
      value: "ap-southeast-1"
    },
    { name: "Sydney",
      value: "ap-southeast-2"
    },
    { name: "Tokyo",
      value: "ap-northeast-1"
    },
    { name: "South America (Sao Paulo)",
      value: "sa-east-1"
    }
];
bounceConstants.googleRegions = {
  'ZONAL': [
    { name: "Eastern Asia-Pacific",
      value: "ASIA-EAST1"
    },
    { name: "Central US (1)",
      value: "US-CENTRAL1"
    },
    { name: "Central US (2)",
      value: "US-CENTRAL2"
    },
    { name: "Eastern US (1)",
      value: "US-EAST1"
    },
    { name: "Eastern US (2)",
      value: "US-EAST2"
    },
    { name: "Eastern US (3)",
      value: "US-EAST3"
    },
    { name: "Western US",
      value: "US-WEST1"
    }
  ],
  'COLD': [
    { name: "Asia",
      value: "ASIA"
    },
    { name: "Europe",
      value: "EU"
    },
    { name: "United States",
      value: "US"
    }
  ],
  'GLOBAL': [
    { name: "Asia",
      value: "ASIA"
    },
    { name: "Europe",
      value: "EU"
    },
    { name: "United States",
      value: "US"
    }
  ]
};

bounceConstants.googleStorageClasses = [
  { name: 'Standard',
    value: 'GLOBAL'
  },
  { name: 'Nearline',
    value: 'COLD'
  },
  { name: 'Durable reduced availability',
    value: 'ZONAL'
  }
];

bounceConstants.providers = [
  { name: "Amazon S3",
    value: "aws-s3",
    regions: bounceConstants.awsRegions,
    hasRegion: true,
    hasEndpoint: false,
    region: null
  },
  { name: "Google Cloud Storage",
    value: "google-cloud-storage",
    hasRegion: true,
    hasEndpoint: false,
    regions: bounceConstants.googleRegions,
    region: null,
    storageClasses: bounceConstants.googleStorageClasses
  },
  /*
  { name: "Microsoft Azure",
      value: "azure"
    } */
  { name: "OpenStack Swift",
    value: "openstack-swift",
    hasRegion: true,
    regions: [],
    hasEndpoint: true,
    region: null,
    endpoint: null
  },
  { name: "Local filesystem (debugging only)",
    value: "filesystem",
    hasRegion: false,
    hasEndpoint: true,
    endpoint: null
  },
  { name: "In-memory store (debugging only)",
    value: "transient",
    hasRegion: false,
    hasEndpoint: false
  }
];

bounceConstants.getProvider = function(providerValue) {
  for (var i = 0; i < bounceConstants.providers.length; i++) {
    if (bounceConstants.providers[i].value === providerValue) {
      return bounceConstants.providers[i];
    }
  }
  return null;
};

bounceConstants.getRegionName = function(regions, search_region) {
  for (var i = 0; i < regions.length; i++) {
    var region = regions[i];
    if (region.value === search_region) {
      return region.name;
    }
  }
  return null;
};

bounceConstants.getCloudContext = function(provider, store) {
  if (provider.regions === null) {
    return null;
  }
  if (store.storageClass === null) {
    return bounceConstants.getRegionName(provider.regions, store.region);
  }

  var search_regions = null;
  var storage_class = null;
  for (var i = 0; i < provider.storageClasses.length; i++) {
    storage_class = provider.storageClasses[i];
    if (storage_class.value === store.storageClass) {
      search_regions = provider.regions[storage_class.value];
      break;
    }
  }
  if (search_regions === null) {
    return null;
  }
  return storage_class.name + " - " +
         bounceConstants.getRegionName(search_regions, store.region);
};

bounceConstants.tiers = {};
bounceConstants.tiers.ORIGIN = { name: "originLocation",
                                 displayName: "primary"
                               };
bounceConstants.tiers.ARCHIVE = { name: "archiveLocation",
                                  displayName: "archive"
                                };
bounceConstants.tiers.MIGRATION = { name: "migrationTargetLocation",
                                    displayName: "migration target"
                                  };
bounceConstants.tiers.CACHE = { name: "cacheLocation",
                                displayName: "cache"
                              };

bounceConstants.createLocationMap = function(virtualContainer) {
  var locationMap = {};
  if (virtualContainer.cacheLocation.blobStoreId > 0) {
    locationMap['NEAR'] = bounceConstants.tiers.CACHE.displayName;
    locationMap['FAR'] = bounceConstants.tiers.ORIGIN.displayName;
    if (virtualContainer.archiveLocation.blobStoreId > 0) {
      locationMap['FARTHER'] = bounceConstants.tiers.ARCHIVE.displayName;
    }
  } else if (virtualContainer.migrationTargetLocation.blobStoreId > 0) {
    locationMap['NEAR'] = bounceConstants.tiers.MIGRATION.displayName;
    locationMap['FAR'] = bounceConstants.tiers.ORIGIN.displayName;
  } else {
    locationMap['NEAR'] = bounceConstants.tiers.ORIGIN.displayName;
    if (virtualContainer.archiveLocation.blobStoreId > 0) {
      locationMap['FAR'] = bounceConstants.tiers.ARCHIVE.displayName;
    }
  }
  return locationMap;
};

bounceConstants.translateLocations = function(locationMap, object) {
  var result = [];
  for (var i = 0; i < object.regions.length; i++) {
    result.push(locationMap[object.regions[i]]);
  }
  return result.join(", ");
};
