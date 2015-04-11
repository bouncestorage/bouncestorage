var storesControllers = angular.module('storesControllers', ['bounce']);

storesControllers.controller('CreateStoreCtrl', ['$scope', '$location',
  '$timeout', '$routeParams', 'ObjectStore',
  function ($scope, $location, $timeout, $routeParams, ObjectStore) {
    $scope.actions = {};
    $scope.provider = "none";

    ObjectStore.query(function(results) {
      $scope.stores = results;
    });

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

function findStore(stores, id) {
  for (var i = 0; i < id; i++) {
    if (stores[i].id == id) {
      return stores[i];
    }
  }
  return undefined;
}

storesControllers.controller('ViewStoresCtrl', ['$scope', '$location',
  '$timeout', '$routeParams', 'ObjectStore', 'Container',
  function ($scope, $location, $timeout, $routeParams, ObjectStore, Container) {
    $scope.actions = {};
    ObjectStore.query(function(results) {
      $scope.stores = results;
      if ($routeParams.id !== null) {
        $scope.store = findStore($scope.stores, $routeParams.id);
        if ($scope.store === undefined) {
          $scope.store = $scope.stores[0];
        }
      } else {
        $scope.store = $scope.stores[0];
      }

      if ($scope.store !== undefined) {
        Container.query({ id: $scope.store.id }, function(results) {
          $scope.containers = results;
        });
      }
    });

    $scope.actions.addStore = function() {
      $location.path("/create_store/false");
    };

    $scope.actions.editStore = function(store) {
      $location.path("/edit_store/" + store.id);
    };

    $scope.interpretStatus = function(containerStatus) {
      if (containerStatus === 'UNCONFIGURED') {
        return 'passthrough';
      }
      if (containerStatus === 'CONFIGURED') {
        return 'enhanced';
      }
    };
}]);
