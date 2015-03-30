var dashboardControllers = angular.module('dashboardControllers', ['bounce']);

dashboardControllers.controller('DashboardCtrl', ['$scope', '$q', '$location',
    '$timeout', 'VirtualContainer',
    function ($scope, $q, $location, $timeout, VirtualContainer) {
  $scope.actions = {};
  VirtualContainer.query(function(results) {
    $scope.containers = results;
  });

  $scope.actions.configureContainer = function(container) {
    console.log("configuring: " + container);
  };

  $scope.actions.addContainer = function() {
    $location.path("/create_container");
  };

}]);
