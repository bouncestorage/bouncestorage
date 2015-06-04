var BounceUtils = {};
BounceUtils.awsRegions = [
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
BounceUtils.googleRegions = {
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

BounceUtils.googleStorageClasses = [
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

BounceUtils.providers = [
  { name: "Amazon S3",
    value: "aws-s3",
    regions: BounceUtils.awsRegions,
    hasRegion: true,
    hasEndpoint: false,
    region: null
  },
  { name: "Google Cloud Storage",
    value: "google-cloud-storage",
    hasRegion: true,
    hasEndpoint: false,
    regions: BounceUtils.googleRegions,
    region: null,
    storageClasses: BounceUtils.googleStorageClasses
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

BounceUtils.getProvider = function(providerValue) {
  for (var i = 0; i < BounceUtils.providers.length; i++) {
    if (BounceUtils.providers[i].value === providerValue) {
      return BounceUtils.providers[i];
    }
  }
  return null;
};

BounceUtils.getRegionName = function(regions, search_region) {
  for (var i = 0; i < regions.length; i++) {
    var region = regions[i];
    if (region.value === search_region) {
      return region.name;
    }
  }
  return null;
};

BounceUtils.getCloudContext = function(provider, store) {
  if (provider.regions === null) {
    return null;
  }
  if (store.storageClass === null) {
    return BounceUtils.getRegionName(provider.regions, store.region);
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
         BounceUtils.getRegionName(search_regions, store.region);
};

BounceUtils.tiers = {
  ORIGIN:
    { name: "originLocation",
      displayName: "primary"
    },
  ARCHIVE:
    { name: "archiveLocation",
      displayName: "archive"
    },
  MIGRATION:
    { name: "migrationTargetLocation",
      displayName: "migration target"
    },
  CACHE:
    { name: "cacheLocation",
      displayName: "cache"
    }
};

BounceUtils.createLocationMap = function(virtualContainer) {
  var locationMap = {};
  if (virtualContainer.cacheLocation.blobStoreId > 0) {
    locationMap['NEAR'] = BounceUtils.tiers.CACHE.displayName;
    locationMap['FAR'] = BounceUtils.tiers.ORIGIN.displayName;
    if (virtualContainer.archiveLocation.blobStoreId > 0) {
      locationMap['FARTHER'] = BounceUtils.tiers.ARCHIVE.displayName;
    }
  } else if (virtualContainer.migrationTargetLocation.blobStoreId > 0) {
    locationMap['NEAR'] = BounceUtils.tiers.MIGRATION.displayName;
    locationMap['FAR'] = BounceUtils.tiers.ORIGIN.displayName;
  } else {
    locationMap['NEAR'] = BounceUtils.tiers.ORIGIN.displayName;
    if (virtualContainer.archiveLocation.blobStoreId > 0) {
      locationMap['FAR'] = BounceUtils.tiers.ARCHIVE.displayName;
    }
  }
  return locationMap;
};

BounceUtils.translateLocations = function(locationMap, object) {
  var result = [];
  for (var i = 0; i < object.regions.length; i++) {
    result.push(locationMap[object.regions[i]]);
  }
  return result.join(", ");
};

BounceUtils.findStore = function(stores, id) {
  for (var i = 0; i < stores.length; i++) {
    if (stores[i].id === id) {
      return stores[i];
    }
  }
  return undefined;
};

BounceUtils.durationUnits = ['seconds', 'minutes', 'hours', 'days', 'months',
    'years'];

BounceUtils.isSet = function(x) {
  return (x !== undefined) && (x !== null);
};

BounceUtils.toDuration = function(value, units) {
  var durationSetting = {};
  durationSetting[units] = value;
  return moment.duration(durationSetting).toJSON();
};

BounceUtils.setDuration = function(tier) {
  if (BounceUtils.isSet(tier.moveDuration) &&
      BounceUtils.isSet(tier.moveUnits)) {
    tier.object.moveDelay = BounceUtils.toDuration(tier.moveDuration,
        tier.moveUnits);
  }
  if (BounceUtils.isSet(tier.copyDuration) &&
      BounceUtils.isSet(tier.copyUnits)) {
    tier.object.copyDelay = BounceUtils.toDuration(tier.copyDuration,
        tier.copyUnits);
  }
};

BounceUtils.parseDuration = function(durationString, object, valueField,
    unitsField) {
  if (!BounceUtils.isSet(durationString) || durationString === '') {
    return;
  }

  var duration = moment.duration(durationString);
  for (var i = 0; i < BounceUtils.durationUnits.length; i++) {
    var index = BounceUtils.durationUnits.length - i - 1;
    var unit = BounceUtils.durationUnits[index];
    if (duration[unit]() > 0) {
      var nextUnit = BounceUtils.durationUnits[index + 1];
      if (duration[nextUnit]() === 0) {
        object[valueField] = duration[unit]();
        object[unitsField] = unit;
        return;
      }
    }
  }
};

BounceUtils.parseDurations = function(tierLocation) {
  BounceUtils.parseDuration(tierLocation.object.copyDelay, tierLocation,
      'copyDuration', 'copyUnits');
  BounceUtils.parseDuration(tierLocation.object.moveDelay, tierLocation,
      'moveDuration', 'moveUnits');
};

BounceUtils.toHumanSize = function(dataSize) {
  var sizes = [
    { name: 'kB',
      size: 1024
    },
    { name: 'MB',
      size: 1024*1024
    },
    { name: 'GB',
      size: 1024*1024*1024
    },
    { name: 'TB',
      size: 1024*1024*1024*1024
    }
  ];

  for (var i = 0; i < sizes.length; i++) {
    var convertedSize = dataSize / sizes[i].size;
    if (convertedSize > 1) {
      // Keep at most three digits for each size
      if (convertedSize > 100) {
        return Math.round(convertedSize) + "  " + sizes[i].name;
      } else if (convertedSize > 10) {
        return Math.round(convertedSize * 10) / 10 + " " + sizes[i].name;
      } else {
        return Math.round(convertedSize * 100) / 100 + " " + sizes[i].name;
      }
    }
  }

  return dataSize;
};
