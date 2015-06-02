var dashboardControllers = angular.module('dashboardControllers', ['bounce',
    'nvd3ChartDirectives']);

var DB_NAME = "bounce";
var DB_URL = "http://" + window.location.hostname + ":8086";
var DB_USER = "bounce";
var DB_PASSWORD = "bounce";
var SERIES_URL = DB_URL + "/db/" + DB_NAME + "/series?u=" + DB_USER +
    "&p=" + DB_PASSWORD;

var TRACKED_METHODS = { 'PUT': 0,
                        'GET': 1,
                        'DELETE': 2
                      };

var OPS_SERIES_PREFIX = "ops";
var OPS_QUERY = "select count(object) from merge(/^" + OPS_SERIES_PREFIX +
    "\\./i) group by time(30s) fill(0)";
var DURATION_QUERY = "select mean(duration) from merge(/^" + OPS_SERIES_PREFIX;
var DURATION_PARAMETERS = " group by time(30s) fill(0) where time > now() - 1h";

var OBJECT_STORE_SERIES = "object_stores";
var OBJECT_STORES_QUERY = "select sum(objects), sum(object_size) from " +
    OBJECT_STORE_SERIES + " group by provider";

function opCountQuery(op) {
  return OP_COUNT_SIZE_QUERY + "'" + op + "'";
}

function durationQuery(opName) {
  return DURATION_QUERY + "\\..*\.op\." + opName + "$/) " +
      DURATION_PARAMETERS;
}

dashboardControllers.controller('DashboardCtrl', ['$scope', '$location',
    '$http', '$interval', '$filter', 'ObjectStore',
    function($scope, $location, $http, $interval, $filter, ObjectStore) {
  $scope.opsData = [{ key: "Number of operations",
                      values: []
                    }];
  $scope.objectStores = null;
  $scope.total_space = null;
  $scope.totalObjectStoreData = [];

  $scope.durationData = [];
  for (var i in TRACKED_METHODS) {
    $scope.durationData.push({ key: i,
                               values: []
                             });
  }

  $scope.formatDate = function() {
    return function(epoch) {
      return $filter('date')(epoch, 'hh:mm:ss');
    };
  };

  $scope.formatDuration = function() {
    return function(duration) {
      return Math.round(duration*100)/100;
    };
  };

  $scope.getObjectStoreData = function() {
    var query = OBJECT_STORES_QUERY;
  };

  $scope.getDurationData = function() {
    for (var i in TRACKED_METHODS) {
      $http.get(SERIES_URL, { params: { q: durationQuery(i) } })
        .success((function(method) {
          return function(results) {
            if (results.length === 0 || results[0].points.length === 0) {
              return;
            }
            console.log(results);
            $scope.durationData[TRACKED_METHODS[method]].values =
                results[0].points;
          };
        })(i))
        .error(function(error) {
          console.log(error);
        }
      );
    }
  };

  $scope.getOpsData = function() {
    var query = OPS_QUERY;
    query += " where time > now()-1h";
    $http.get(SERIES_URL, { params: { q: query }
                          })
      .success(function(results) {
        $scope.opsData[0].values = results[0].points;
      }).error(function(error) {
        console.log(error);
      }
    );
  };

  $scope.getOpsData();
  $scope.getDurationData();
  $scope.refreshOpsData = $interval($scope.getOpsData, 5000);
  $scope.refreshDurationData = $interval($scope.getDurationData, 5000);

  $scope.$on('$locationChangeStart', function() {
    if ($scope.refreshOpsData !== null) {
      $interval.cancel($scope.refreshOpsData);
    }
    if ($scope.refreshDurationData !== null) {
      $interval.cancel($scope.refreshDurationData);
    }
  });
}]);
