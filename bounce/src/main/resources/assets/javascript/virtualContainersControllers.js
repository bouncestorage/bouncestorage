var virtualContainersControllers = angular.module('virtualContainersControllers', ['bounce']);

virtualContainersControllers.controller('CreateVirtualContainerCtrl', ['$scope', '$q', '$location',
    '$timeout', 'ObjectStore', 'Container', 'VirtualContainer',
    function ($scope, $q, $location, $timeout, ObjectStore, Container,
        VirtualContainer) {
  $scope.actions = {};
  $scope.store = "none";
  $scope.container = "none";

  ObjectStore.query({}, function(results) {
    $scope.stores = results;
  });

  $scope.actions.storeChanged = function() {
    console.log("Maybe fetching containers " + $scope.store);
    if ($scope.store === "new") {
      $location.path("/create_store");
      return;
    }

    if ($scope.store === "none") {
      $scope.container = "none";
      return;
    }

    Container.get({}, function(results) {
      $scope.containers = results.containerNames;
      console.log($scope.containers);
    });
  };

  $scope.actions.createContainer = function() {
    VirtualContainer.save({
        name: $scope.name,
        originBlobStoreId: $scope.store,
        originContainerName: $scope.container
      },
      function(result) {
        $location.path("/dashboard");
      },
      function(result) {
        console.log("Error: " + result);
      }
    );
  };

}]);
