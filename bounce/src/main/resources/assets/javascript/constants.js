var bounceConstants = {};
bounceConstants.awsRegions = [ { name: "US Standard",
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
bounceConstants.googleRegions = [ { name: "Eastern Asia-Pacific",
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
                                ];

bounceConstants.providers = [ { name: "Amazon S3",
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
                                region: null
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
    if (bounceConstants.providers[i].value == providerValue) {
      return bounceConstants.providers[i];
    }
  }
  return null;
};

bounceConstants.getRegion = function(provider, region) {
  if (provider.regions === null) {
    return null;
  }
  for (var i = 0; i < provider.regions.length; i++) {
    if (provider.regions[i].value === region) {
      return provider.regions[i];
    }
  }
  return null;
};
