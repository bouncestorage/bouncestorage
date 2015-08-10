var dashboardControllers = angular.module('dashboardControllers', ['bounce',
    'nvd3ChartDirectives']);

dashboardControllers.controller('DashboardCtrl', ['$rootScope', '$scope',
    '$location', '$http', '$interval', '$filter', 'ObjectStore',
    'objectStoreStats',
    function($rootScope, $scope, $location, $http, $interval, $filter,
        ObjectStore, objectStoreStats) {
  $scope.opsData = [{ key: "Number of operations",
                      values: []
                    }];
  $scope.objectStores = null;
  $scope.total_space = null;
  $scope.totalObjectStoreData = [];

  $scope.durationData = [];
  for (var i in BounceUtils.TRACKED_METHODS) {
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

  $rootScope.$on('objectStoreStatsComplete', function() {
    console.log("stats done!");
    $scope.totalObjectStoreData = objectStoreStats.result;
  });

  ObjectStore.query(
    function(result) {
      $scope.objectStores = result;
      objectStoreStats.getStats(result);
    },
    function(error) {
      console.log("Error fetching object stores: ");
      console.log(error);
    }
  );

  $scope.updatePieChart = function() {
    $scope.pieChartRequests--;
    if ($scope.pieChartRequests === 0) {
      $scope.totalObjectStoreData = $scope.updatedPieChart;
    }
  };

  $scope.getObjectStoreData = function() {
    // We aggregate the pie chart data into this array
    $scope.updatedPieChart = [];
    // We need to keep track of the outstanding requests and only update the
    // chart when the last one completes
    $scope.pieChartRequests = 0;
  };

  $scope.objectStoreName = function() {
    return function(d) {
      return d.key;
    };
  };

  $scope.objectStoreSize = function() {
    return function(d) {
      return d.data.size;
    };
  };

  $scope.getDurationData = function() {
    for (var i in BounceUtils.TRACKED_METHODS) {
      $http.get(BounceUtils.SERIES_URL,
          { params: { query: BounceUtils.durationQuery(i) }
          })
        .success((function(method) {
          return function(results) {
            if (results.length === 0 || results[0].points.length === 0) {
              return;
            }
            $scope.durationData[BounceUtils.TRACKED_METHODS[method]].values =
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
    $http.get(BounceUtils.SERIES_URL, { params: { query: BounceUtils.OPS_QUERY }
                          })
      .success(function(results) {
        if (results.length === 0 || results[0].points.length === 0) {
          return;
        }
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
