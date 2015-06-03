var bounce = angular.module('bounce', [
  'ngResource',
  'ngRoute',
  'dashboardControllers',
  'settingsControllers',
  'storesControllers',
  'welcomeControllers'
]);

bounce.config(['$locationProvider', function($locationProvider) {
    $locationProvider.html5Mode(false);
}]);

bounce.config(['$routeProvider', function($routeProvider) {
  $routeProvider.
    when('/create_store/:objectStoreId?', {
      templateUrl: 'views/partials/create_store.html',
      controller: 'CreateStoreCtrl'
    }).
    when('/dashboard', {
      templateUrl: 'views/partials/dashboard.html',
      controller: 'DashboardCtrl'
    }).
    when('/edit_store/:objectStore', {
      redirectTo: '/create_store/:objectStore'
    }).
    when('/stores/:id?', {
      templateUrl: 'views/partials/stores.html',
      controller: 'ViewStoresCtrl'
    }).
    when('/settings', {
      templateUrl: 'views/partials/settings.html',
      controller: 'SettingsCtrl'
    }).
    when('/welcome', {
      templateUrl: 'views/partials/welcome.html',
      controller: 'WelcomeCtrl'
    }).
    otherwise({
      redirectTo: '/welcome'
    });
}]);

bounce.factory('Container', ['$resource', function($resource) {
  return $resource('/api/object_store/:id/container/:name', { id: "@id" });
}]);

bounce.factory('VirtualContainer', ['$resource', function($resource) {
  return $resource('/api/virtual_container/:id', { id: "@id" },
    { 'update': { method: 'PUT'}
    });
}]);

bounce.factory('ObjectStore', ['$resource', function($resource) {
  return $resource('/api/object_store/:id', { id: "@id" },
    { 'update': { method: 'PUT'}
    });
}]);

bounce.factory('BounceService', ['$resource', function($resource) {
  return $resource('/api/bounce/:name', { name: "@name" });
}]);

bounce.factory('AboutBuild', ['$resource', function($resource) {
    return $resource('/api/about/build');
}]);

bounce.controller('NavBarCtrl', ['$scope', '$rootScope', '$location',
    'ObjectStore', 'AboutBuild',
    function ($scope, $rootScope, $location, ObjectStore, AboutBuild) {
  $scope.stores = {};
  ObjectStore.query(function(results) {
    $scope.stores = results;
  });
  AboutBuild.get(function(results) {
      $scope.about = results;
  });

  $rootScope.$on('addedStore', function(event, store) {
    ObjectStore.query(function(results) {
      $scope.stores = results;
    });
  });

  $scope.isActive = function(path) {
    return $location.path() === path;
  };

  $scope.isActivePrefix = function(path) {
    return $location.path().startsWith(path);
  };
}]);
