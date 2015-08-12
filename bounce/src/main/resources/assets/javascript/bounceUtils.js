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

BounceUtils.capacityUnits = ['GB', 'TB', 'PB'];
BounceUtils.capacityMap = { GB: 1024*1024*1024,
                            TB: 1024*1024*1024*1024,
                            PB: 1024*1024*1024*1024*1024
                          };

BounceUtils.isSet = function(x) {
  return (x !== undefined) && (x !== null);
};

BounceUtils.toDuration = function(value, units) {
  var durationSetting = {};
  durationSetting[units] = value;
  return moment.duration(durationSetting).toJSON();
};

BounceUtils.setDuration = function(tier) {
  if (tier.moveUnits === null || tier.moveUnits === undefined) {
    tier.object.moveDelay = '-P1D';
  } else if (BounceUtils.isSet(tier.moveDuration) &&
      BounceUtils.isSet(tier.moveUnits)) {
    tier.object.moveDelay = BounceUtils.toDuration(tier.moveDuration,
        tier.moveUnits);
  }

  if (tier.copyUnits === null || tier.copyUnits === undefined) {
    tier.object.copyDelay = '-P1D';
  } else if (BounceUtils.isSet(tier.copyDuration) &&
      BounceUtils.isSet(tier.copyUnits)) {
    tier.object.copyDelay = BounceUtils.toDuration(tier.copyDuration,
        tier.copyUnits);
  }
};

BounceUtils.setCapacity = function(tier) {
  if (tier.capacityUnits === null || tier.capacityUnits === undefined) {
    tier.object.capacity = null;
  } else if (BounceUtils.isSet(tier.capacityUnits) &&
      BounceUtils.isSet(tier.capacity)) {
    var unitMultiplier = BounceUtils.capacityMap[tier.capacityUnits];
    tier.object.capacity = unitMultiplier * Number(tier.capacity);
  }
};

BounceUtils.parseCapacity = function(tier) {
  var capacity = tier.object.capacity;
  if (!BounceUtils.isSet(capacity) || capacity === '') {
    return;
  }
  var size = BounceUtils.toHumanSize(capacity);
  var tokens = size.split(' ');
  tier.capacity = tokens[0];
  tier.capacityUnits = tokens[tokens.length - 1];
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
    if (duration[unit]() < 0) {
      object[valueField] = "";
      object[unitsField] = null;
    }
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

BounceUtils.parseFields = function(tierLocation) {
  BounceUtils.parseDuration(tierLocation.object.copyDelay, tierLocation,
      'copyDuration', 'copyUnits');
  BounceUtils.parseDuration(tierLocation.object.moveDelay, tierLocation,
      'moveDuration', 'moveUnits');
  BounceUtils.parseCapacity(tierLocation);
};

BounceUtils.validateTier = function(tier) {
  var moveSet = tier.object.moveDelay && tier.object.moveDelay !== '-P1D';
  var copySet = tier.object.copyDelay && tier.object.copyDelay !== '-P1D';
  var capacitySet = tier.object.capacity && tier.object.capacity > 0;
  if (!moveSet && ! copySet && !capacitySet) {
    return 'Either a storage limit or copy/eviction time must bet set';
  }
  if (moveSet && capacitySet) {
    return 'Cannot set both storage limit and eviction time';
  }

  return null;
};

BounceUtils.toHumanSize = function(dataSize) {
  var sizes = [
    { name: 'KB',
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
    },
    { name: 'PB',
      size: 1024*1024*1024*1024*1024
    }
  ];

  for (var i = 0; i < sizes.length; i++) {
    var sizeEntry = sizes[sizes.length - i - 1];
    var convertedSize = dataSize / sizeEntry.size;
    if (convertedSize > 1) {
      // Keep at most three digits for each size
      if (convertedSize > 100) {
        return Math.round(convertedSize) + "  " + sizeEntry.name;
      } else if (convertedSize > 10) {
        return Math.round(convertedSize * 10) / 10 + " " + sizeEntry.name;
      } else {
        return Math.round(convertedSize * 100) / 100 + " " + sizeEntry.name;
      }
    }
  }

  return dataSize;
};

BounceUtils.queryTagsMap = {
  ops: {
    serie: 0,
    provider: 2,
    container: 4,
    op: 6
  },
  object_store_stats: {
    serie: 0,
    provider: 2,
    container: 4
  }
};

BounceUtils.parseSerieName = function (name) {
  var tokens = name.split(".");
  var map = BounceUtils.queryTagsMap[tokens[0]];
  var keys = Object.keys(map);
  var result = {};
  for (var i = 0; i < keys.length; i++) {
    result[keys[i]] = tokens[map[keys[i]]];
  }
  return result;
};

BounceUtils.DB_NAME = "bounce";
BounceUtils.DB_URL = "/api/db/";
BounceUtils.DB_USER = "bounce";
BounceUtils.DB_PASSWORD = "bounce";
BounceUtils.SERIES_URL = BounceUtils.DB_URL + BounceUtils.DB_NAME +
    "?username=" + BounceUtils.DB_USER + "&password=" + BounceUtils.DB_PASSWORD;

BounceUtils.TRACKED_METHODS =
  { 'PUT': 0,
    'GET': 1,
    'DELETE': 2
  };

BounceUtils.OPS_SERIES_PREFIX = "ops";
BounceUtils.OPS_QUERY = "select count(object) from merge(/^" +
    BounceUtils.OPS_SERIES_PREFIX + "./i) group by time(30s) fill(0) " +
    "where time > now()-1h";
BounceUtils.DURATION_QUERY = "select mean(duration) from merge(/^" +
    BounceUtils.OPS_SERIES_PREFIX;
BounceUtils.DURATION_PARAMETERS =
    " group by time(30s) fill(0) where time > now() - 1h";

BounceUtils.OBJECT_STORE_PREFIX = "object_store_stats"
BounceUtils.OBJECT_STORE_QUERY =
    "select * from /^" + BounceUtils.OBJECT_STORE_PREFIX;

BounceUtils.OPS_QUERY_OBJECTS = "select * from /^" +
    BounceUtils.OPS_SERIES_PREFIX;

BounceUtils.opCountQuery = function(op) {
  return BounceUtils.OP_COUNT_SIZE_QUERY + "'" + op + "'";
};

BounceUtils.durationQuery = function(opName) {
  return BounceUtils.DURATION_QUERY + "..*\.op\." + opName + "$/) " +
      BounceUtils.DURATION_PARAMETERS;
};

BounceUtils.objectStoreStatsQuery = function(providerId) {
  return BounceUtils.OBJECT_STORE_QUERY + "\.provider\." +
      providerId + "..*/ limit 1";
};

BounceUtils.containerStatsQuery = function(providerId, containerName, method) {
  return BounceUtils.OPS_QUERY_OBJECTS + ".provider." + providerId +
      ".container." + containerName + ".op." + method + '$/';
};


BounceUtils.QUERY_FIELDS = {
  CONTAINER_STATS:
    { time: 'time',
      seq: 'sequence_number',
      objects: 'objects',
      size: 'size'
    },
  OP_STATS:
    { time: 'time',
      seq: 'sequence_number',
      duration: 'duration',
      key: 'object',
      size: 'size'
    }
};

BounceUtils.InfluxDBParser = function(fields, columns) {
  function createGetter(key) {
    return function(point) { return point[this[key + '_field']]; };
  }

  for (var i = 0; i < columns.length; i++) {
    for (var key in fields) {
      if (fields[key] === columns[i]) {
        this[key + '_field'] = i;
        this['get_' + key] = createGetter(key);
      }
    }
  } 
};
