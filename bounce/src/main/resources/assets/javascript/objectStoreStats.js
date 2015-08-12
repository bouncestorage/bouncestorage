var bounce = angular.module('bounce');
bounce.factory('objectStoreStats', ['$rootScope', '$http',
  function($rootScope, $http) {
    var pendingRequests = 0;
    var objectStoreData = [];

    function finishRequest() {
      pendingRequests--;
      if (pendingRequests === 0) {
        $rootScope.$broadcast('objectStoreStatsComplete');
      }
    }

    function createObjectStoreData(nickname) {
      return { key: nickname,
               data: { size: 0,
                       objects: 0
                     }
             };
    }

    function aggregateValues(results) {
      var observedValues = {};
      var totalSize = 0;
      var parser = new BounceUtils.InfluxDBParser(
          BounceUtils.QUERY_FIELDS.OP_STATS, results.columns);
      for (var i = 0; i < results.points.length; i++) {
        var point = results.points[i];
        var key = parser.get_key(point);
        // InfluxDB returns the values in chronological order and we rely on
        // that here
        if (key in observedValues) {
          continue;
        }
        observedValues[key] = true;
        totalSize += Number(parser.get_size(point));
      }
      return totalSize;
    }

    function updateTotalContainerStats(objectStore, container, since, data,
        otherContainers) {
      var lookups = [ { method: 'PUT',
                        callback: function(results) {
                          data.data.size += aggregateValues(results);
                        }
                      },
                      { method: 'DELETE',
                        callback: function(results) {
                          data.data.size -= aggregateValues(results);
                        }
                      }
                    ];
      pendingRequests += lookups.length;
      for (var i = 0; i < lookups.length; i++) {
        var method = lookups[i].method;
        var callback = lookups[i].callback;
        var query = BounceUtils.containerStatsQuery(objectStore.id, container,
            method);
        if (since !== null) {
          query += " where time > " + since;
        }
        $http.get(BounceUtils.SERIES_URL, { params: { query: query } })
            .success(function(callback) {
              return function(result) {
                finishRequest();
                if (result.length === 0 || result[0].points.length === 0) {
                  return;
                }
                for (var j = 0; j < result.length; j++) {
                  var tags = BounceUtils.parseSerieName(result[j].name);
                  // This function may be called with "*" for container name, in
                  // which case we need to filter out any containers we have
                  // already retrieved statistics for.
                  if (otherContainers.indexOf(tags.container) < 0) {
                    callback(result[j]);
                  }
                }
              }
            }(callback))
            .error(function(error) {
              console.log(error);
            });
      }
    }

    function processContainerStats(data, objectStore, results) {
      var containers = [];
      var updateTimes = [];
      for (var j = 0; j < results.length; j++) {
        var serieTags = BounceUtils.parseSerieName(results[j].name);
        var parser = new BounceUtils.InfluxDBParser(
            BounceUtils.QUERY_FIELDS.CONTAINER_STATS, results[j].columns);
        data.data.size += parser.get_size(results[j].points[0]);
        data.data.objects += parser.get_objects(results[j].points[0]);
        containers.push(serieTags.container);
        updateTimes.push(parser.get_time(results[j].points[0]));
      }
      // For each container, we need to add/subtract added/deleted object sizes
      // since the last time statistics were gathered
      for (var j = 0; j < results.length; j++) {
        // We do not pass the known containers list, as we query for
        // a specific container
        updateTotalContainerStats(objectStore, containers[j], updateTimes[j],
            data, []);
      }
      // Add any containers that we do not have stats for
      updateTotalContainerStats(objectStore, ".*", null, data, containers);
    }

    function getObjectStoreStats(objectStores) {
      objectStoreData.length = 0;

      for (var i = 0; i < objectStores.length; i++) {
        var objectStore = objectStores[i];
        var query = BounceUtils.objectStoreStatsQuery(objectStore.id);
        pendingRequests++;
        $http.get(BounceUtils.SERIES_URL, { params: { query: query } })
            .success(function(objectStore) {
              var data = createObjectStoreData(objectStore.nickname);
              objectStoreData.push(data);
              return function(results) {
                finishRequest();

                if (results.length === 0) {
                  updateTotalContainerStats(objectStore, ".*", null, data, []);
                  return;
                }
                processContainerStats(data, objectStore, results);
              }
            }(objectStore))
            .error(function(error) {
              console.log(error);
            });
      }
    }

    return { getStats: getObjectStoreStats,
             result: objectStoreData
           };
  }
]);
