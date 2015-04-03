/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

public final class VirtualContainer {
    public enum TIER { CACHE, PRIMARY, ARCHIVE, MIGRATION };
    public static final String TIER_PREFIX = "tier.";
    public static final String PRIMARY_TIER_PREFIX = TIER_PREFIX + TIER.PRIMARY.ordinal();
    public static final String CACHE_TIER_PREFIX = TIER_PREFIX + TIER.CACHE.ordinal();
    public static final String ARCHIVE_TIER_PREFIX = TIER_PREFIX + TIER.ARCHIVE.ordinal();
    public static final String MIGRATION_TIER_PREFIX = TIER_PREFIX + TIER.MIGRATION.ordinal();
    public static final String NAME = "name";

    private String name;
    private int id;
    private Location originLocation;

    private Location cacheLocation;
    private Location archiveLocation;
    private Location migrationTargetLocation;

    public VirtualContainer() {
        originLocation = new Location();
        archiveLocation = new Location();
        migrationTargetLocation = new Location();
        cacheLocation = new Location();
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setOriginLocation(Location location) {
        this.originLocation.setLocation(location);
    }

    public Location getOriginLocation() {
        return originLocation;
    }

    public String toString() {
        return "Name: " + name + " ID: " + id + " originLocation: " + originLocation.toString();
    }

    public Location getCacheLocation() {
        return cacheLocation;
    }

    public void setCacheLocation(Location cacheLocation) {
        this.cacheLocation = cacheLocation;
    }

    public Location getArchiveLocation() {
        return archiveLocation;
    }

    public void setArchiveLocation(Location archiveLocation) {
        this.archiveLocation = archiveLocation;
    }

    public Location getMigrationTargetLocation() {
        return migrationTargetLocation;
    }

    public void setMigrationTargetLocation(Location migrationTargetLocation) {
        this.migrationTargetLocation = migrationTargetLocation;
    }
}
