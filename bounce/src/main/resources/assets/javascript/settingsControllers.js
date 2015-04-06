var settingsControllers = angular.module('settingsControllers', ['bounce']);

settingsControllers.controller('SettingsCtrl', ['$scope', '$location',
  '$timeout', '$routeParams', 'Settings',
  function ($scope, $location, $timeout, $routeParams, Settings) {
      $scope.actions = {};
      $scope.error = "";
      $scope.result = "";

      Settings.get({}, function(result) {
          $scope.s3Address = result.s3Address;
          $scope.s3Port = result.s3Port;
      });

      $scope.actions.update = function() {
          $scope.result = $scope.error = "";
          new Settings({
              s3Address: $scope.s3Address,
              s3Port: $scope.s3Port
          }).$save(null, function() {
              $scope.result = "Updated";
          }, function(error) {
              $scope.error = error.data.message;
              console.log("Error: " + JSON.stringify(error, 4));
          });
      };
}]);

bounce.factory('Settings', ['$resource', function($resource) {
  return $resource('/api/settings/:settings', { settings : "@settings" },
    { 'update': { method: 'POST'}
    });
}]);
