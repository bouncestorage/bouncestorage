var settingsControllers = angular.module('settingsControllers', ['bounce']);

settingsControllers.controller('SettingsCtrl', ['$scope', '$location',
  '$timeout', '$routeParams', 'Settings',
  function ($scope, $location, $timeout, $routeParams, Settings) {
      $scope.actions = {};
      $scope.error = "";
      $scope.result = "";
      $scope.s3Enabled = false;
      $scope.swiftEnabled = false;

      Settings.get({}, function(result) {
          $scope.s3Address = result.s3Address;
          $scope.s3Port = result.s3Port;
          if ($scope.s3Port < 0) {
            $scope.s3Port = null;
          }
          $scope.swiftAddress = result.swiftAddress;
          $scope.swiftPort = result.swiftPort;
          if ($scope.swiftPort < 0) {
            $scope.swiftPort = null;
          }
          console.log(result);
          $scope.s3Enabled = (result.s3Address !== null &&
            result.s3Address !== "" && result.s3Port > 0);
          $scope.swiftEnabled = (result.swiftAddress !== null &&
            result.swiftAddress !== "" && result.swiftPort > 0);
      });

      $scope.actions.update = function() {
          $scope.result = $scope.error = "";
          var settings = { s3Address: $scope.s3Address,
                           s3Port: $scope.s3Port,
                           swiftAddress: $scope.swiftAddress,
                           swiftPort: $scope.swiftPort
                         };
          if (!$scope.s3Enabled) {
            settings.s3Address = null;
            settings.s3Port = -1;
          }
          if (!$scope.swiftEnabled) {
            settings.swiftAddress = null;
            settings.swiftPort = -1;
          }
          new Settings(settings).$save(null, function() {
              $scope.result = "Updated";
              console.log($scope.result);
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
