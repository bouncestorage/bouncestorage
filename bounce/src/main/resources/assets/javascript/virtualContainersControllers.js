var virtualContainersControllers = angular.module('virtualContainersControllers', ['bounce']);

virtualContainersControllers.controller('ViewContainersCtrl', ['$scope', '$location', '$interval',
    'VirtualContainer', 'ObjectStore', 'BounceService',
    function ($scope, $location, $interval, VirtualContainer, ObjectStore, BounceService) {
  $scope.actions = {};
  $scope.objectStoreMap = {};
  $scope.refreshBounce = null;
  VirtualContainer.query(function(results) {
    $scope.containers = results;
  });

  ObjectStore.query(function(results) {
    for (var i = 0; i < results.length; i++) {
      $scope.objectStoreMap[results[i].id] = results[i].nickname;
    }
  });

  $scope.describeLocation = function(locationObject) {
    if (locationObject.blobStoreId < 0 || locationObject.containerName == null) {
      return "Not configured";
    }
    return $scope.objectStoreMap[locationObject.blobStoreId] + "/" +
      locationObject.containerName;
  };

  $scope.actions.configureContainer = function(container) {
    $location.path("/edit_container/" + container.id);
  };

  $scope.actions.addContainer = function() {
    $location.path("/create_container");
  };

  var refreshBounceState = function() {
    var $allBouncing = $('.bouncing');
    if ($allBouncing.length == 0) {
      $interval.cancel($scope.refreshBounce);
      $scope.refreshBounce = null;
      return;
    }
    for (var i = 0; i < $allBouncing.length; i++) {
      var $button = $allBouncing[i];
      var name = $button.id.substring("bounce-btn-".length);
      BounceService.get({ name: name }, function(result) {
        if (result.endTime !== null) {
          $('#bounce-btn-' + name).removeClass('disabled').removeClass('bouncing').html('Bounce');
        }
      }, function(error) {
        console.log(error);
      });
    }
  };

  $scope.actions.bounce = function(container) {
    var $bounceBtn = $('#bounce-btn-' + container.name);
    $bounceBtn.addClass('disabled');
    BounceService.save({ name: container.name }, function(result) {
      $bounceBtn.html('Bouncing...');
      $bounceBtn.addClass('bouncing');
      if ($scope.refreshBounce === null) {
        $scope.refreshBounce = $interval(refreshBounceState, 1000);
      }
    },
    function(result) {
      $bounceBtn.removeClass('disabled');
    });
  };

  $scope.$on('$locationChangeStart', function() {
    if ($scope.refreshBounce !== null) {
      $interval.cancel($scope.refreshBounce);
    }
  });

}]);

virtualContainersControllers.controller('CreateVirtualContainerCtrl', ['$scope',
    '$location', 'ObjectStore', 'Container', 'VirtualContainer',
    function ($scope, $location, ObjectStore, Container, VirtualContainer) {
  $scope.actions = {};
  $scope.store = "none";
  $scope.container = "none";

  ObjectStore.query({}, function(results) {
    $scope.stores = results;
  });

  $scope.actions.storeChanged = function() {
    if ($scope.store === "new") {
      $location.path("/create_store");
      return;
    }

    if ($scope.store === "none") {
      $scope.container = "none";
      return;
    }

    Container.query({ id: $scope.store }, function(results) {
      $scope.containers = [];
      for (var i = 0; i < results.length; i++) {
        $scope.containers.push(results[i].name);
      }
    });
  };

  $scope.actions.cancel = function() {
    $location.path("/dashboard");
  };

  $scope.actions.createContainer = function() {
    VirtualContainer.save({
        name: $scope.name,
        originLocation: { blobStoreId: $scope.store,
                          containerName: $scope.container
                        }
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

virtualContainersControllers.controller('EditVirtualContainerCtrl', ['$scope',
    '$location', '$routeParams', 'ObjectStore', 'Container', 'VirtualContainer',
    function ($scope, $location, $routeParams, ObjectStore, Container,
        VirtualContainer) {
  $scope.actions = {};
  $scope.utils = {};
  $scope.stores = [{ id: -1,
                     nickname: "Select an object store.."
                   }];
  $scope.containersMap = {};
  $scope.storeNameMap = {};

  $scope.actions.updateContainerMap = function(blobStoreId) {
    $scope.containersMap[blobStoreId] = [];
    Container.query({ id: blobStoreId }, function(results) {
      for (var i = 0; i < results.length; i++) {
        $scope.containersMap[blobStoreId].push(results[i].name);
      }
    }); 
  };
  
  $scope.utils.setContainerNames = function(locations) {
    for (var i = 0; i < locations.length; i++) {
      if (locations[i].containerName === null) {
        locations[i].containerName = "";
        continue;
      }
    }
  };

  VirtualContainer.get({ id: $routeParams.containerId },
    function(container) {
      $scope.name = container.name;
      $scope.originLocation = container.originLocation;
      $scope.cacheLocation = container.cacheLocation;
      $scope.migrationTargetLocation = container.migrationTargetLocation;
      $scope.archiveLocation = container.archiveLocation;
      $scope.allLocations = [ $scope.originLocation,
                              $scope.cacheLocation,
                              $scope.migrationTargetLocation,
                              $scope.archiveLocation
                            ];
      $scope.utils.setContainerNames($scope.allLocations);

      var setStores = {};
      for (var i = 0; i < $scope.allLocations.length; i++) {
        if ($scope.allLocations[i].blobStoreId < 0) {
          continue;
        }

        if ($scope.allLocations[i].blobStoreId in setStores) {
          continue;
        }

        setStores[$scope.allLocations[i].blobStoreId] = true;
        $scope.actions.updateContainerMap($scope.allLocations[i].blobStoreId);
      }
    },
    function(error) {
      $location.path("/dashboard");
    }
  );

  ObjectStore.query(function(results) {
    $scope.stores = $scope.stores.concat(results);
    for (var i = 0; i < results.length; i++) {
      $scope.storeNameMap[results[i].id] = results[i].nickname;
    }
  });

  $scope.actions.cancel = function() {
    $location.path("/dashboard");
  };

  $scope.actions.editContainer = function() {
    VirtualContainer.update({
      id: $routeParams.containerId,
      cacheLocation: $scope.cacheLocation,
      originLocation: $scope.originLocation,
      archiveLocation: $scope.archiveLocation,
      migrationTargetLocation: $scope.migrationTargetLocation,
      name: $scope.name
    }, function(result) {
      $location.path("/dashboard");
    }, function(error) {
      console.log(error);
    });
  };
}]);
