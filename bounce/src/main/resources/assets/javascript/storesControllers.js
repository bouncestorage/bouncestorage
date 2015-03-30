var storesControllers = angular.module('storesControllers', ['bounce']);

storesControllers.controller('CreateStoreCtrl', ['$scope', '$location',
  '$timeout', '$routeParams', 'ObjectStore',
  function ($scope, $location, $timeout, $routeParams, ObjectStore) {
    $scope.actions = {};
    $scope.provider = "none";

    $scope.actions.submitNewStore = function() {
      ObjectStore.save(
        { nickname: $scope.nickname,
          provider: $scope.provider,
          identity: $scope.identity,
          credential: $scope.credential,
          region: $scope.region,
          endpoint: $scope.endpoint
        }, function (res) {
          console.log($routeParams);
          if ($routeParams.welcomeUrl === 'welcome') {
            $location.path("/dashboard");
          } else {
            $location.path("/stores");
          }
        });
    };

    $scope.actions.updatePrompts = function () {
      console.log($scope.provider);
    };
}]);

storesControllers.controller('ViewStoresCtrl', ['$scope', '$location',
  '$timeout', 'ObjectStore',
  function ($scope, $location, $timeout, ObjectStore) {
    $scope.actions = {};
    ObjectStore.query(function(results) {
      $scope.stores = results;
    });

    $scope.actions.addStore = function() {
      $location.path("/create_store");
    };

    $scope.actions.editStore = function() {
      $location.path("/");
    };
}]);
