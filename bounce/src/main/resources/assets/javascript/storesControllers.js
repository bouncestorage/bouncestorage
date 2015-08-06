var storesControllers = angular.module('storesControllers', ['bounce']);

storesControllers.controller('CreateStoreCtrl', ['$scope', '$rootScope',
  '$location', '$routeParams', 'ObjectStore',
  function ($scope, $rootScope, $location, $routeParams, ObjectStore) {
    $scope.actions = {};
    $scope.provider = {};
    $scope.providers = BounceUtils.providers;

    if (typeof($routeParams.objectStoreId) === 'string') {
      $scope.edit = true;
      ObjectStore.get({ id:$routeParams.objectStoreId },
        function(result) {
          $scope.nickname = result.nickname;
          for (var i = 0; i < $scope.providers.length; i++) {
            if ($scope.providers[i].value === result.provider) {
              $scope.provider = $scope.providers[i];
            }
          }
          $scope.provider.identity = result.identity;
          $scope.provider.credential = result.credential;
          $scope.provider.region = result.region;
          $scope.provider.endpoint = result.endpoint;
          $scope.provider.class = result.storageClass;
      });
    }

    $scope.actions.submitNewStore = function() {
      var newStore = { nickname: $scope.nickname,
                       provider: $scope.provider.value,
                       identity: $scope.provider.identity,
                       credential: $scope.provider.credential,
                       region: $scope.provider.region,
                       endpoint: $scope.provider.endpoint,
                       storageClass: $scope.provider.class
                     };
      ObjectStore.save(newStore, function (successStore) {
        $rootScope.$emit('addedStore', successStore);
        $location.path('/stores/' + successStore.id);
      });
    };

    $scope.actions.updateStore = function() {
      ObjectStore.update({
        id: $routeParams.objectStoreId,
        nickname: $scope.provider.nickname,
        provider: $scope.provider.value,
        identity: $scope.provider.identity,
        credential: $scope.provider.credential,
        region: $scope.provider.region,
        endpoint: $scope.provider.endpoint
      }, function(res) {
        $location.path('/stores');
      }, function(error) {
        console.log(error);
      });
    };

    $scope.actions.cancelEdit = function() {
      $location.path('/stores');
    };
}]);

function createNewVirtualContainer(store, container) {
  var newVirtualContainer = { name: container.name };
  angular.forEach(BounceUtils.tiers, function(tier) {
    newVirtualContainer[tier.name] = { blobStoreId: -1,
                                       containerName: '',
                                       copyDelay: '',
                                       moveDelay: ''
                                     };
    if (tier === BounceUtils.tiers.ORIGIN) {
      newVirtualContainer[tier.name].blobStoreId = store.id;
      newVirtualContainer[tier.name].containerName = container.name;
    }
  });
  return newVirtualContainer;
}

function extractLocations(vContainer) {
  return [{ name: 'a cache',
            edit_name: 'cache',
            action_label: 'from the cache',
            tier: BounceUtils.tiers.CACHE,
            object: vContainer.cacheLocation
          },
          { name: 'an archive',
            edit_name: 'archive',
            action_label: 'to the archive',
            tier: BounceUtils.tiers.ARCHIVE,
            object: vContainer.archiveLocation
          },
          { name: 'a migration target',
            edit_name: 'migration',
            action_label: 'to the migration target',
            tier: BounceUtils.tiers.MIGRATION,
            object: vContainer.migrationTargetLocation
          }];
}

function setArchiveFields(vContainer, toPrimary) {
  // HACK: We need to copy the copyDelay and moveDelay settings from the
  // archive location to the origin location (or vice versa) to present these
  // settings correctly on edits (and to save the edits correctly).
  var archive = vContainer.archiveLocation;
  if (archive.blobStoreId !== -1) {
    var primary = vContainer.originLocation;
    if (toPrimary) {
      primary.moveDelay = archive.moveDelay;
      primary.copyDelay = archive.copyDelay;
      primary.capacity = archive.capacity;
      archive.moveDelay = null;
      archive.copyDelay = null;
      archive.capacity = null;
    } else {
      archive.moveDelay = primary.moveDelay;
      archive.copyDelay = primary.copyDelay;
      archive.capacity = primary.capacity;
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
    $scope.newContainer = null;
    $scope.providerLabel = null;
    $scope.durationUnits = BounceUtils.durationUnits;
    $scope.capacityUnits = BounceUtils.capacityUnits;
    $scope.tiers = BounceUtils.tiers;

    $scope.getProviderLabel = function() {
      if ($scope.store.region === null) {
        return $scope.provider.name;
      } else {
        return $scope.provider.name + " (" +
          BounceUtils.getCloudContext($scope.provider, $scope.store) + ")";
      }
    };

    $scope.refreshContainersMap = function() {
      for (var i = 0; i < $scope.stores.length; i++) {
        $scope.updateContainerMap($scope.stores[i].id);
      }
    };

    ObjectStore.query(function(results) {
      $scope.stores = results;
      var redirect = true;
      if ($routeParams.id !== null) {
        $scope.store = BounceUtils.findStore($scope.stores,
            Number($routeParams.id));
        if ($scope.store !== undefined) {
          redirect = false;
        }
      }
      if (redirect === true) {
        if ($scope.stores.length > 0) {
          $location.path('/stores/' + $scope.stores[0].id);
        } else {
          $location.path('/create_store');
        }
      } else {
        $scope.provider = BounceUtils.getProvider($scope.store.provider);
        $scope.providerLabel = $scope.getProviderLabel();
        $scope.refreshContainersMap();
      }
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
      if (!(blobStoreId in $scope.containersMap)) {
        return [];
      }
      if (blobStoreId === $scope.enhanceContainer.originLocation.blobStoreId) {
        return $scope.containersMap[blobStoreId].filter(
          function(container) {
            return (container.status === 'UNCONFIGURED' &&
                    container.name !== $scope.enhanceContainer.name) ||
                   (container.status === 'INUSE' &&
                    container.name === editLocation.containerName);
          });
      } else {
        return $scope.containersMap[blobStoreId].filter(
          function(container) {
            return container.status === 'UNCONFIGURED';
          });
      }
    };

    $scope.actions.enhanceContainer = function(container) {
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
                             setArchiveFields(vContainer, false);
                             $scope.locations = extractLocations(vContainer);
                             $('#configureContainerModal').modal('show');
                           }
                          );
      return;
    };

    $scope.isLocationConfigurable = function(vLocation) {
      if ($scope.enhanceContainer === null) {
        return false;
      }

      if (vLocation.tier === BounceUtils.tiers.MIGRATION) {
        var archive = $scope.enhanceContainer[BounceUtils.tiers.ARCHIVE.name];
        if (archive.blobStoreId >= 0) {
          return true;
        }
        var cache = $scope.enhanceContainer[BounceUtils.tiers.CACHE.name];
        if (cache.blobStoreId >= 0) {
          return true;
        }

        return false;
      }

      var migration = $scope.enhanceContainer[BounceUtils.tiers.MIGRATION.name];
      return migration.blobStoreId >= 0;
    };

    $scope.listVirtualContainer = function(virtualContainer) {
      var tierName = BounceUtils.tiers.ORIGIN.name;
      var id = virtualContainer[tierName].blobStoreId;
      var container = virtualContainer[tierName].containerName;
      Container.get({ id: id, name: container },
                    function(result) {
                      var map = BounceUtils.createLocationMap(virtualContainer);
                      for (var i = 0; i < result.objects.length; i++) {
                        var object = result.objects[i];
                        object.locations = BounceUtils.translateLocations(map, object);
                        object.size = BounceUtils.toHumanSize(object.size);
                      }
                      $scope.listedContainer = result;
                    },
                    function(error) {
                      console.log(error);
                    });
    };

    $scope.getName = function(container) {
      if (container.name.length > 22) {
        return container.name.substring(0, 19) + '...';
      }
      return container.name;
    };

    $scope.actions.listContainer = function(container) {
      $scope.listedContainer = {};
      if (container.status === 'CONFIGURED') {
        VirtualContainer.get({ id: container.virtualContainerId },
                              function(result) {
                                $scope.listVirtualContainer(result);
                              },
                              function(error) {
                                console.log(error);
                              }
                            );
      } else {
        Container.get({ id: $scope.store.id,
                        name: container.name
                      },
                      function(result) {
                        for (var i = 0; i < result.objects.length; i++) {
                          result.objects[i].locations =
                              BounceUtils.tiers.ORIGIN.displayName;
                          result.objects[i].size = BounceUtils.toHumanSize(
                              result.objects[i].size);
                        }
                        $scope.listedContainer = result;
                      },
                      function(error) {
                        console.log(error);
                      });
      }
      $('#listContainerModal').modal('show');
    };

    $scope.actions.prompt = function(locationObject) {
      $scope.editLocation = locationObject;
      BounceUtils.parseFields($scope.editLocation);
      $('#configureTierModal').modal('show');
    };

    $scope.actions.updateTier = function() {
      BounceUtils.setDuration($scope.editLocation);
      BounceUtils.setCapacity($scope.editLocation);
      var validationResult = BounceUtils.validateTier($scope.editLocation);
      if (validationResult !== null) {
        $scope.editLocation.message = validationResult;
        return;
      }
      $('#configureTierModal').modal('hide');
    };

    $scope.actions.saveContainer = function() {
      setArchiveFields($scope.enhanceContainer, true);
      if ($scope.enhanceContainer.id === undefined) {
        VirtualContainer.save($scope.enhanceContainer,
        function(result) {
          console.log('Saved container: ' + result.status);
          $scope.refreshContainersMap();
        },
        function(error) {
          console.log(error);
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

    $scope.actions.addContainer = function() {
      Container.save({ id: $scope.store.id, name: $scope.newContainer },
        function(result) {
          $scope.updateContainerMap($scope.store.id);
        },
        function(error) {
          console.log(error);
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
