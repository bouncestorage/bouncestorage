var storesControllers = angular.module('storesControllers', ['bounce']);

storesControllers.controller('CreateStoreCtrl', ['$scope', '$location',
  '$routeParams', 'ObjectStore',
  function ($scope, $location, $routeParams, ObjectStore) {
    $scope.actions = {};
    $scope.provider = 'none';

    ObjectStore.query(function(results) {
      $scope.stores = results;
    });

    if (typeof($routeParams.objectStoreId) === 'string') {
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
            $location.path('/dashboard');
          } else {
            $location.path('/stores');
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
        $location.path('/stores');
      }, function(error) {
        console.log('Error: ' + error);
      });
    };

    $scope.actions.updatePrompts = function () {
      // TODO: dynamically load prompts for various providers
      console.log($scope.provider);
    };

    $scope.actions.cancelEdit = function() {
      $location.path('/stores');
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

function createNewVirtualContainer(store, container) {
  return { cacheLocation: { blobStoreId: -1,
                            containerName: '',
                            copyDelay: '',
                            moveDelay: ''
                          },
           originLocation: { blobStoreId: store.id,
                             containerName: container.name,
                             copyDelay: '',
                             moveDelay: ''
                           },
           archiveLocation: { blobStoreId: -1,
                              containerName: '',
                              copyDelay: '',
                              moveDelay: ''
                            },
           migrationTargetLocation: { blobStoreId: -1,
                                      containerName: '',
                                      copyDelay: '',
                                      moveDelay: ''
                                    },
           name: container.name,
         };
}

function extractLocations(vContainer) {
  return [{ name: 'a cache',
            edit_name: 'cache',
            object: vContainer.cacheLocation
          },
          { name: 'an archive',
            edit_name: 'archive',
            object: vContainer.archiveLocation
          },
          { name: 'a migration target',
            edit_name: 'migration',
            object: vContainer.migrationTargetLocation
          }];
}

function setArchiveDuration(vContainer, toPrimary) {
  // HACK: We need to copy the copyDelay and moveDelay settings from the
  // archive location to the origin location (or vice versa) to present these
  // settings correctly on edits (and to save the edits correctly).
  var archive = vContainer.archiveLocation;
  if (archive.blobStoreId !== -1) {
    var primary = vContainer.originLocation;
    if (toPrimary) {
      primary.moveDelay = archive.moveDelay;
      primary.copyDelay = archive.copyDelay;
      archive.moveDelay = '';
      archive.copyDelay = '';
    } else {
      archive.moveDelay = primary.moveDelay;
      archive.copyDelay = primary.copyDelay;
    }
  }
  return;
}

storesControllers.controller('ViewStoresCtrl', ['$scope', '$location',
  '$interval', '$routeParams', 'ObjectStore', 'Container', 'VirtualContainer',
  'BounceService', function ($scope, $location, $interval, $routeParams,
      ObjectStore, Container, VirtualContainer, BounceService) {
    $scope.actions = {};
    $scope.locations = [];
    $scope.containersMap = {};
    $scope.refreshBounce = null;

    $scope.refreshContainersMap = function() {
      for (var i = 0; i < $scope.stores.length; i++) {
        $scope.updateContainerMap($scope.stores[i].id);
      }
    };

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
      $scope.refreshContainersMap();
    });

    $scope.updateContainerMap = function(blobStoreId) {
      $scope.containersMap[blobStoreId] = [];
      Container.query({ id: blobStoreId }, function(results) {
        for (var i = 0; i < results.length; i++) {
          $scope.containersMap[blobStoreId].push(results[i]);
        }
        if (blobStoreId === $scope.store.id) {
          $scope.containers = $scope.containersMap[$scope.store.id].filter(
            function(container) {
              return container.status !== 'INUSE';
            });
        }
      });
    };

    $scope.getContainersForPrompt = function() {
      if ($scope.editLocation === null || $scope.editLocation === undefined) {
        return [];
      }
      var editLocation = $scope.editLocation.object;
      var blobStoreId = editLocation.blobStoreId;
      if (!(blobStoreId in $scope.containers)) {
        return [];
      }
      return $scope.containersMap[blobStoreId].filter(
        function(container) {
          return (container.status === 'UNCONFIGURED' &&
                  container.name !== $scope.enhanceContainer.name) ||
                 (container.status === 'INUSE' &&
                  container.name === editLocation.containerName);
        });
    };

    $scope.actions.enhanceContainer = function(container) {
      console.log(container);
      if (container.status === 'UNCONFIGURED') {
        var vContainer = createNewVirtualContainer($scope.store, container);
        $scope.locations = extractLocations(vContainer);
        $scope.enhanceContainer = vContainer;
        $('#configureContainerModal').modal('show');
        return;
      }

      VirtualContainer.get({ id: container.virtualContainerId },
                           function(vContainer) {
                             $scope.enhanceContainer = vContainer;
                             console.log($scope.enhanceContainer);
                             setArchiveDuration(vContainer, false);
                             $scope.locations = extractLocations(vContainer);
                             $('#configureContainerModal').modal('show');
                           }
                          );
      return;
    };

    $scope.actions.prompt = function(locationObject) {
      $scope.editLocation = locationObject;
      $('#configureTierModal').modal('show');
    };

    $scope.actions.saveContainer = function() {
      setArchiveDuration($scope.enhanceContainer, true);
      console.log($scope.enhanceContainer);
      if (typeof($scope.enhanceContainer.id) === 'undefined') {
        VirtualContainer.save($scope.enhanceContainer,
        function(result) {
          console.log('Saved container: ' + result.status);
          $scope.refreshContainersMap();
        },
        function(error) {
          console.log('Error: ' + error);
        });
      } else {
        VirtualContainer.update($scope.enhanceContainer,
        function(success) {
          console.log('Updated container: ' + success.status);
          $scope.refreshContainersMap();
        },
        function(error) {
          console.log('Error occurred during the update: ' + error);
        });
      }
      $('#configureContainerModal').modal('hide');
      $scope.enhanceContainer = null;
      $scope.editLocation = null;
    };

    $scope.actions.editStore = function(store) {
      $location.path('/edit_store/' + store.id);
    };

    $scope.interpretStatus = function(containerStatus) {
      if (containerStatus === 'UNCONFIGURED') {
        return 'passthrough';
      }
      if (containerStatus === 'CONFIGURED') {
        return 'enhanced';
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
      function(error) {
        console.log(error);
        $bounceBtn.removeClass('disabled');
      });
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
            $('#bounce-btn-' + name).removeClass('disabled').removeClass('bouncing').html('bounce!');
          }
        }, function(error) {
          console.log(error);
        });
      }
    };

    $scope.$on('$locationChangeStart', function() {
      if ($scope.refreshBounce !== null) {
        $interval.cancel($scope.refreshBounce);
      }
    });
}]);
