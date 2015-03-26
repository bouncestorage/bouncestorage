var bounce = angular.module('bounce', [])
    

var bounceControllers = angular.module('bounceControllers', []);

bounceControllers.controller('BounceCtrl',
               ['$scope', '$http', '$q', '$location', '$timeout',
function ($scope, $http, $q, $location, $timeout)
{
    $scope.actions = {};
    $scope.options = {};
    $scope.status = {};
    $scope.handles = {};
    $scope.errors = {};

    $http.get("/api/service").then(
        function(res) {
            $scope.buckets = res.data.containerNames;
            if (!$scope.options.bucketSelect && $scope.buckets) {
                $scope.options.bucketSelect = $scope.buckets[0];
            }
            $scope.errors.buckets = null;
        }
    ).catch(
        function(res) {
            $scope.errors.buckets = res.data.message;
        }
    );

    $scope.actions.listBlobs = function() {
        $http.get("/api/container?name=" + $scope.options.bucketSelect).then(
            function(res) {
                $scope.blobs = {}
                $scope.blobs.names = res.data.blobNames;
                $scope.blobs.linkCount = res.data.bounceLinkCount;
                $scope.errors.listBlobs = null;
            }
        ).catch(
            function(res) {
                $scope.errors.listBlobs = res.data.message;
            }
        );
    };

    $scope.actions.bounceBlobs = function() {
        $http.post("/api/bounce?name=" + $scope.options.bucketSelect).then(
            function(res) {
                console.log("Bounce blobs response received");
                $scope.errors.bounceBlobs = null;
                $scope.actions.status();
            }
        ).catch(
            function(res) {
                $scope.errors.bounceBlobs = res.data.message;
            }
        );
    };

    $scope.actions.status = function() {
        $timeout.cancel($scope.handles.status);

        $http.get("/api/bounce").then(
            function(res) {
                $scope.status = res.data;
                if (!$scope.status.every(function(status) { return status.done; })) {
                    $scope.handles.status = $timeout($scope.actions.status, 1 * 1000/*ms*/);
                }
                $scope.errors.status = null;
            }
        ).catch(
            function(res) {
                $scope.errors.status = res.data.message;
            }
        );
    };
    $scope.actions.status();
}]);
