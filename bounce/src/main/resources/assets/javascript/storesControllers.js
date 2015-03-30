var storesControllers = angular.module('storesControllers', ['bounce']);

storesControllers.controller('CreateStoreCtrl', ['$scope', '$location',
  '$timeout', '$routeParams', 'ObjectStore',
  function ($scope, $location, $timeout, $routeParams, ObjectStore) {
    $scope.actions = {};
    $scope.provider = "none";

    if (typeof($routeParams.objectStoreId) === "string") {
      $scope.edit = true;
      ObjectStore.get({ id:$routeParams.objectStoreId },
        function(result) {
          $scope.nickname = result.nickname;
          $scope.provider = result.provider;
          $scope.identity = result.identity;
          $scope.credential = result.credential;
          $scope.region = result.region;
          $scope.endpoint = result.endpoint;
      });
    }

    $scope.actions.submitNewStore = function() {
      ObjectStore.save(
        { nickname: $scope.nickname,
          provider: $scope.provider,
          identity: $scope.identity,
          credential: $scope.credential,
          region: $scope.region,
          endpoint: $scope.endpoint
        }, function (res) {
          if ($routeParams.welcomeUrl === 'welcome') {
            $location.path("/dashboard");
          } else {
            $location.path("/stores");
          }
        });
    };

    $scope.actions.updateStore = function() {
      ObjectStore.update({
        id: $routeParams.objectStoreId,
        nickname: $scope.nickname,
        provider: $scope.provider,
        identity: $scope.identity,
        credential: $scope.credential,
        region: $scope.region,
        endpoint: $scope.endpoint
      }, function(res) {
        $location.path("/stores");
      }, function(error) {
        console.log("Error: " + error);
      });
    };

    $scope.actions.updatePrompts = function () {
      // TODO: dynamically load prompts for various providers
      console.log($scope.provider);
    };

    $scope.actions.cancelEdit = function() {
      $location.path("/stores");
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
      $location.path("/create_store/false");
    };

    $scope.actions.editStore = function(store) {
      $location.path("/edit_store/" + store.id);
    };
}]);
