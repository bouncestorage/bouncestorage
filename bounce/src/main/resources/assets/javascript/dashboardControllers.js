var dashboardControllers = angular.module('dashboardControllers', ['bounce',
    'nvd3ChartDirectives']);

var DB_NAME = "bounce";
var DB_URL = "http://localhost:8086";
var DB_USER = "bounce";
var DB_PASSWORD = "bounce";
var SERIES_URL = DB_URL + "/db/" + DB_NAME + "/series?u=" + DB_USER +
    "&p=" + DB_PASSWORD;
var SERIES_NAME = "ops";
var DURATION_QUERY = "select mean(duration) from " + SERIES_NAME +
    " group by time(30s), op fill(0)";
var DURATION_SERIES = { 'PUT': 0,
                        'GET': 1
                      };
var OPS_QUERY = "select count(op) from " + SERIES_NAME +
    " group by time(30s) fill(0)";

dashboardControllers.controller('DashboardCtrl', ['$scope', '$location',
    '$http', '$interval', '$filter', function($scope, $location, $http, $interval,
    $filter) {
  $scope.opsData = [{ key: "Number of operations",
                      values: []
                    }];

  $scope.durationData = [];
  for (var i in DURATION_SERIES) {
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

  $scope.getDurationData = function() {
    var query = DURATION_QUERY + " where time > now()-1h";
    $http.get(SERIES_URL, { params: { q: query }
                          })
      .success(function(results) {
        if (results.length === 0 || results[0].points.length === 0) {
          return;
        }
        for (key in DURATION_SERIES) {
          var series_id = DURATION_SERIES[key];
          var keySeries = results[0].points.filter(function(e) {
            return e[2] === key;
          });
          if (keySeries.length !== 0) {
            $scope.durationData[series_id].values = keySeries;
          }
        }
      }).error(function(error) {
        console.log(error);
      }
    );
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
