/*global angular*/
/*global bounce*/

var settingsControllers = angular.module('settingsControllers', ['bounce']);

settingsControllers.controller('SettingsCtrl', ['$scope', '$location',
  '$timeout', '$routeParams', 'Settings',
  function ($scope, $location, $timeout, $routeParams, Settings) {
      $scope.actions = {};
      $scope.error = "";
      $scope.result = "";
      $scope.s3Enabled = false;
      $scope.swiftEnabled = false;

      function refresh() {
          Settings.get({}, function(result) {
              console.log(result);
              $scope.s3Address = result.s3Address;
              $scope.s3Port = result.s3Port;
              $scope.s3SSLAddress = result.s3SSLAddress;
              $scope.s3SSLPort = result.s3SSLPort;
              $scope.s3Domain = result.s3Domain;
              $scope.domainCertificate = result.domainCertificate;
              if ($scope.s3Port < 0) {
                  $scope.s3Port = null;
              }
              if ($scope.s3SSLPort < 0) {
                  $scope.s3SSLPort = null;
              }
              $scope.swiftAddress = result.swiftAddress;
              $scope.swiftPort = result.swiftPort;
              if ($scope.swiftPort < 0) {
                  $scope.swiftPort = null;
              }
              $scope.s3Enabled = (result.s3Address && result.s3Port >= 0) ||
                  (result.s3SSLAddress && result.s3SSLPort >= 0);
              if (!$scope.s3Enabled) {
                  $scope.s3Address = "0.0.0.0";
                  $scope.s3SSLAddress = "0.0.0.0";
                  $scope.s3Port = 80;
                  $scope.s3SSLPort = 443;
              }
              $scope.swiftEnabled = (result.swiftAddress !== null &&
                                     result.swiftAddress !== "" && result.swiftPort > 0);
              if (!$scope.swiftEnabled) {
                  $scope.swiftAddress = "0.0.0.0";
                  $scope.swiftPort = 8080;
              }
          });
      }
      refresh();

      $scope.actions.update = function() {
          $scope.result = $scope.error = "";
          var settings = { s3Address: $scope.s3Address,
                           s3Port: $scope.s3Port,
                           s3SSLAddress: $scope.s3SSLAddress,
                           s3SSLPort: $scope.s3SSLPort,
                           s3Domain: $scope.s3Domain,
                           swiftAddress: $scope.swiftAddress,
                           swiftPort: $scope.swiftPort
                         };
          if (!$scope.s3Enabled) {
            settings.s3Address = null;
            settings.s3SSLAddress = null;
            settings.s3Port = -1;
            settings.s3SSLPort = -1;
          }
          if (!$scope.swiftEnabled) {
            settings.swiftAddress = null;
            settings.swiftPort = -1;
          }
          new Settings(settings).$save(null, function() {
              $scope.result = "Updated";
              console.log($scope.result);
              refresh();
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
