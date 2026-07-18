package net.coreprotect.command;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.bukkit.Bukkit;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;

import net.coreprotect.CoreProtect;
import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.consumer.Consumer;
import net.coreprotect.consumer.process.Process;
import net.coreprotect.database.Database;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;

public final class PlayProMigrationCommand {

    private static final String MIGRATION_RESOURCE = "/migration/playpro-clickhouse-migration.sql";
    private static final List<String> ARCHIVE_TABLES = List.of(
            "art_map", "block", "chat", "command", "container", "entity_container",
            "entity_interaction", "item", "database_lock", "entity", "entity_spawn",
            "entity_map", "material_map", "blockdata_map", "session", "sign", "skull",
            "user", "username_log", "version", "world");
    private static final List<String> PHYSICAL_TARGET_TABLES = List.of(
            "storage_metadata", "writer_registration", "retention_high_water", "event_data");
    private static final List<String> MIGRATED_FAMILIES = List.of(
            "art_map", "block", "chat", "command", "container", "entity_container",
            "entity_interaction", "item", "entity", "entity_spawn", "entity_map",
            "material_map", "blockdata_map", "session", "sign", "skull", "user",
            "username_log", "world");
    private static final Set<String> FINAL_SOURCE_FAMILIES = Set.of(
            "block", "container", "entity_container", "entity_interaction", "item", "entity_spawn");
    private static final Set<String> ID_ROWID_SOURCE_FAMILIES = Set.of(
            "art_map", "entity_map", "material_map", "blockdata_map", "world");
    private static final Set<String> STRICT_ROWID_FAMILIES = Set.of(
            "art_map", "block", "container", "entity_container", "entity_interaction",
            "item", "entity", "entity_spawn", "entity_map", "material_map",
            "blockdata_map", "user", "world");
    private static final Set<String> HISTORICAL_DUPLICATE_ROWID_FAMILIES = Set.of(
            "chat", "command", "session", "sign", "skull", "username_log");
    private static final List<ExpectedColumn> EVENT_DATA_COLUMNS = List.of(
            new ExpectedColumn("event_data", "dataset_id", "UUID"),
            new ExpectedColumn("event_data", "producer_id", "UUID"),
            new ExpectedColumn("event_data", "producer_sequence", "UInt64"),
            new ExpectedColumn("event_data", "batch_id", "UUID"),
            new ExpectedColumn("event_data", "batch_ordinal", "UInt32"),
            new ExpectedColumn("event_data", "family", "LowCardinality(String)"),
            new ExpectedColumn("event_data", "rowid", "UInt64"),
            new ExpectedColumn("event_data", "payload", "Nullable(String)"),
            new ExpectedColumn("event_data", "meta", "Nullable(String)"),
            new ExpectedColumn("event_data", "blockdata", "Nullable(String)"),
            new ExpectedColumn("event_data", "metadata", "Nullable(String)"),
            new ExpectedColumn("event_data", "entity_data", "Nullable(String)"),
            new ExpectedColumn("event_data", "entity_data_present", "Nullable(UInt8)"));
    private static final List<ExpectedColumn> BINARY_VIEW_COLUMNS = List.of(
            new ExpectedColumn("block", "meta", "Array(Int8)"),
            new ExpectedColumn("block", "blockdata", "Array(Int8)"),
            new ExpectedColumn("container", "metadata", "Array(Int8)"),
            new ExpectedColumn("entity_container", "metadata", "Array(Int8)"),
            new ExpectedColumn("entity_interaction", "metadata", "Array(Int8)"),
            new ExpectedColumn("item", "data", "Array(Int8)"),
            new ExpectedColumn("entity", "data", "Array(Int8)"),
            new ExpectedColumn("entity_spawn", "data", "Array(Int8)"));

    private PlayProMigrationCommand() {
        throw new IllegalStateException("Command class");
    }

    protected static void runCommand(CommandSender sender, boolean permission, String[] args) {
        if (!permission) {
            return;
        }
        if (sender instanceof Player) {
            error(sender, "This migration can only be executed from console.");
            return;
        }
        if (!Config.getGlobal().MYSQL) {
            error(sender, "This migration is only available for the ClickHouse storage backend.");
            return;
        }
        if (ConfigHandler.converterRunning || ConfigHandler.migrationRunning || ConfigHandler.purgeRunning) {
            error(sender, "Another database operation is already running.");
            return;
        }
        if (!ConfigHandler.activeRollbacks.isEmpty()) {
            error(sender, "A rollback/restore is currently active. Try again later.");
            return;
        }
        if (!Bukkit.getOnlinePlayers().isEmpty()) {
            error(sender, "Kick all players or run during maintenance before migrating.");
            return;
        }

        MigrationOptions options;
        try {
            options = MigrationOptions.parse(args);
        }
        catch (IllegalArgumentException e) {
            error(sender, e.getMessage());
            usage(sender);
            return;
        }

        if (options.archivePrefix.equals(options.livePrefix)) {
            error(sender, "Archive prefix cannot be the same as the live prefix.");
            return;
        }
        if (options.rebuild && options.sourceDatabase.equals(options.database) && options.sourcePrefix.equals(options.livePrefix)) {
            error(sender, "Rebuild mode requires source-prefix to point at archived old tables, not the live prefix.");
            return;
        }
        if (!options.rebuild && !options.sourceDatabase.equals(options.database)) {
            error(sender, "source-database is only supported with rebuild:true.");
            return;
        }

        ConfigHandler.migrationRunning = true;
        Thread migrationThread = new Thread(() -> runMigration(sender, options));
        migrationThread.setName("CoreProtect PlayPro Migration");
        migrationThread.setUncaughtExceptionHandler((thread, throwable) -> {
            CoreProtect.getInstance().getSLF4JLogger().error("Unhandled PlayPro migration failure", throwable);
            ConfigHandler.migrationRunning = false;
            error(sender, "Migration failed unexpectedly. Logging remains paused; keep the server in maintenance and see console for details.");
        });
        migrationThread.start();
        if (options.rebuild) {
            ok(sender, "Started PlayPro rebuild in " + options.database + " from archived source " + options.sourceDatabase + "." + options.sourcePrefix + "*.");
        }
        else {
            ok(sender, "Started in-place PlayPro migration in " + options.database + ". Old tables will be archived as " + options.archivePrefix + "*.");
        }
    }

    private static void runMigration(CommandSender sender, MigrationOptions options) {
        boolean success = false;
        try {
            waitForConsumerDrain(sender);
            ConfigHandler.pauseConsumer = true;
            ok(sender, "Logging is paused. Do not let players join until you switch to official PlayPro/CoreProtect.");

            try (Connection connection = Database.getConnection(true, 30000)) {
                if (connection == null) {
                    throw new SQLException("Unable to open ClickHouse connection");
                }
                requireClickHouseVersion(connection);
                requireOfficialPlayProAliasSetting(connection, sender);
                requireTargetDatabaseEngine(connection, options);
                boolean resume = isResumableInPlaceMigration(connection, options);
                if (resume) {
                    ok(sender, "Detected a completed data copy from an interrupted migration. Resuming final verification only.");
                    validateEventDataSchema(connection, options.database, options.livePrefix);
                    validateCompatibilityViews(connection, options.database, options.livePrefix);
                }
                else {
                    if (options.rebuild) {
                        requireSourceTables(connection, options);
                        archiveExistingPlayProTarget(connection, options);
                    }
                    else {
                        requireSourceTables(connection, options);
                        requireArchiveTablesFree(connection, options);
                        archiveSourceTables(connection, options);
                    }
                    bootstrapTargetSchema(connection, options);
                    requireTargetReady(connection, options);
                    recreateCompatibilityViews(connection, options.database, options.livePrefix);
                    ok(sender, "Created official PlayPro compatibility views.");
                    runMigrationSql(connection, options, sender);
                }
                reconcileUsernameLogRows(connection, options, sender);
                verifyMigratedRows(connection, options);
                ok(sender, "Verified migrated row counts for every PlayPro event family.");
                PlayProMetadataRepairCommand.repair(connection, options.database, options.livePrefix, sender);
                verifyOfficialLookupShapes(connection, options.database, options.livePrefix);
                ok(sender, "Verified official PlayPro lookup column shapes.");
            }

            success = true;
            ok(sender, "Migration completed successfully.");
            ok(sender, "Stop the server now, replace this jar with official PlayPro/CoreProtect, and keep clickhouse-database: " + options.database + ", table-prefix: " + options.livePrefix);
        }
        catch (Exception e) {
            CoreProtect.getInstance().getSLF4JLogger().error("PlayPro migration failed", e);
            error(sender, "Migration failed: " + e.getMessage());
            error(sender, "Logging remains paused. Keep the server in maintenance until migration or repair succeeds.");
        }
        finally {
            ConfigHandler.migrationRunning = false;
        }
    }

    private static void waitForConsumerDrain(CommandSender sender) throws InterruptedException {
        ok(sender, "Waiting for the current consumer queue to drain...");
        long start = System.currentTimeMillis();
        while (Consumer.getConsumerSize(0) > 0 || Consumer.getConsumerSize(1) > 0 || Process.getCurrentConsumerSize() > 0) {
            if (System.currentTimeMillis() - start > 300000L) {
                throw new IllegalStateException("Timed out while waiting for the consumer queue to drain");
            }
            Thread.sleep(500L);
        }
    }

    private static void requireClickHouseVersion(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery("SELECT version()")) {
            if (!resultSet.next()) {
                throw new SQLException("ClickHouse did not return a version");
            }
            String version = resultSet.getString(1);
            String[] parts = version.split("\\.", 3);
            int major = Integer.parseInt(parts[0]);
            int minor = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
            if (major < 25 || (major == 25 && minor < 6)) {
                throw new SQLException("Official PlayPro/CoreProtect requires ClickHouse 25.6+. Found " + version);
            }
        }
    }

    static void requireOfficialPlayProAliasSetting(Connection connection, CommandSender sender) throws SQLException {
        if (isPreferColumnNameToAliasEnabled(connection)) {
            ok(sender, "Verified ClickHouse prefer_column_name_to_alias=1 for official PlayPro lookup compatibility.");
            return;
        }

        String user = currentClickHouseUser(connection);
        String alterSql = "ALTER USER " + quote(user) + " SETTINGS prefer_column_name_to_alias = 1";
        try (Statement statement = connection.createStatement()) {
            statement.execute(alterSql);
            statement.execute("SET prefer_column_name_to_alias = 1");
        }
        catch (SQLException exception) {
            throw new SQLException("Official PlayPro/CoreProtect ClickHouse lookups require prefer_column_name_to_alias=1 for user "
                    + user + ". Ask your ClickHouse host/admin to run: " + alterSql, exception);
        }

        if (!isPreferColumnNameToAliasEnabled(connection)) {
            throw new SQLException("Unable to enable ClickHouse prefer_column_name_to_alias=1 for the current session. Ask your ClickHouse host/admin to run: " + alterSql);
        }
        ok(sender, "Enabled ClickHouse prefer_column_name_to_alias=1 for user " + user + ".");
    }

    private static boolean isPreferColumnNameToAliasEnabled(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(
                "SELECT value FROM system.settings WHERE name='prefer_column_name_to_alias'")) {
            if (!resultSet.next()) {
                throw new SQLException("ClickHouse did not expose setting prefer_column_name_to_alias");
            }
            return "1".equals(resultSet.getString(1));
        }
    }

    private static String currentClickHouseUser(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery("SELECT currentUser()")) {
            if (!resultSet.next()) {
                throw new SQLException("ClickHouse did not return current user");
            }
            return resultSet.getString(1);
        }
    }

    private static void requireSourceTables(Connection connection, MigrationOptions options) throws SQLException {
        List<String> missing = new ArrayList<>();
        List<String> views = new ArrayList<>();
        String database = options.rebuild ? options.sourceDatabase : options.database;
        String sourcePrefix = options.rebuild ? options.sourcePrefix : options.livePrefix;
        for (String table : ARCHIVE_TABLES) {
            String sourceTable = sourcePrefix + table;
            String engine = tableEngine(connection, database, sourceTable);
            if (engine == null) {
                missing.add(sourcePrefix + table);
            }
            else if ("View".equals(engine)) {
                views.add(sourceTable);
            }
        }
        if (!missing.isEmpty()) {
            String message = "Source database " + database + " is missing required tables: " + String.join(", ", missing)
                    + similarTableHint(connection, database, sourcePrefix);
            if (options.rebuild && !options.sourcePrefix.equals(options.livePrefix)
                    && hasPhysicalSourceTables(connection, options.database, options.livePrefix)) {
                message += ". Old fork tables appear to still be live at " + options.database + "." + options.livePrefix
                        + "*. Run without rebuild: /co migrate-playpro database:" + options.database
                        + " prefix:" + options.livePrefix + " archive-prefix:" + options.archivePrefix;
            }
            throw new SQLException(message);
        }
        if (!views.isEmpty()) {
            String retry = "/co migrate-playpro database:" + options.database + " prefix:" + options.livePrefix + " rebuild:true source-prefix:" + options.archivePrefix;
            throw new SQLException("Source prefix " + sourcePrefix + " points at PlayPro compatibility views, not old fork tables. "
                    + "If a previous migration already archived old tables, run: " + retry);
        }
    }

    private static void requireArchiveTablesFree(Connection connection, MigrationOptions options) throws SQLException {
        List<String> existing = new ArrayList<>();
        for (String table : ARCHIVE_TABLES) {
            if (tableExists(connection, options.database, options.archivePrefix + table)) {
                existing.add(options.archivePrefix + table);
            }
        }
        if (!existing.isEmpty()) {
            throw new SQLException("Archive tables already exist: " + String.join(", ", existing));
        }
    }

    private static boolean hasPhysicalSourceTables(Connection connection, String database, String prefix) throws SQLException {
        for (String table : ARCHIVE_TABLES) {
            String engine = tableEngine(connection, database, prefix + table);
            if (engine == null || "View".equals(engine)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isResumableInPlaceMigration(Connection connection, MigrationOptions options) throws SQLException {
        if (options.rebuild || !options.database.equals(options.sourceDatabase)
                || !options.archivePrefix.equals(options.sourcePrefix)) {
            return false;
        }
        if (!"CoalescingMergeTree".equals(tableEngine(connection, options.database, options.livePrefix + "event_data"))
                || !hasPhysicalSourceTables(connection, options.database, options.archivePrefix)) {
            return false;
        }
        for (String table : ARCHIVE_TABLES) {
            if (!"View".equals(tableEngine(connection, options.database, options.livePrefix + table))) {
                return false;
            }
        }
        return true;
    }

    private static void bootstrapTargetSchema(Connection connection, MigrationOptions options) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (String sql : targetSchema(options)) {
                statement.execute(sql);
            }
        }
    }

    static void recreateCompatibilityViews(Connection connection, String database, String prefix) throws SQLException {
        validateEventDataSchema(connection, database, prefix);
        requireCompatibilityTargetsReplaceable(connection, database, prefix);
        try (Statement statement = connection.createStatement()) {
            for (String sql : compatibilityViewSql(database, prefix)) {
                statement.execute(sql);
            }
        }
        validateCompatibilityViews(connection, database, prefix);
    }

    private static void validateEventDataSchema(Connection connection, String database, String prefix) throws SQLException {
        String table = prefix + "event_data";
        String engine = tableEngine(connection, database, table);
        if (!"CoalescingMergeTree".equals(engine)) {
            throw new SQLException("Official PlayPro event table " + table + " must use CoalescingMergeTree, found " + String.valueOf(engine));
        }
        validateColumns(connection, database, prefix, EVENT_DATA_COLUMNS);
    }

    private static void requireCompatibilityTargetsReplaceable(Connection connection, String database, String prefix) throws SQLException {
        for (String family : ARCHIVE_TABLES) {
            String table = prefix + family;
            String engine = tableEngine(connection, database, table);
            if (engine != null && !"View".equals(engine)) {
                throw new SQLException("Cannot create PlayPro compatibility view " + table + ": existing object uses engine " + engine);
            }
        }
    }

    static void validateCompatibilityViews(Connection connection, String database, String prefix) throws SQLException {
        for (String family : ARCHIVE_TABLES) {
            String table = prefix + family;
            String engine = tableEngine(connection, database, table);
            if (!"View".equals(engine)) {
                throw new SQLException("Missing official PlayPro compatibility view " + table + " (engine=" + String.valueOf(engine) + ")");
            }
        }
        validateColumns(connection, database, prefix, BINARY_VIEW_COLUMNS);
    }

    private static void validateColumns(Connection connection, String database, String prefix, List<ExpectedColumn> expectedColumns) throws SQLException {
        String sql = "SELECT type FROM system.columns WHERE database=? AND table=? AND name=? LIMIT 2";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (ExpectedColumn expected : expectedColumns) {
                String table = prefix + expected.tableSuffix;
                statement.setString(1, database);
                statement.setString(2, table);
                statement.setString(3, expected.name);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        throw new SQLException("Missing required PlayPro column " + table + "." + expected.name);
                    }
                    String actualType = resultSet.getString(1);
                    if (!expected.type.equals(actualType) || resultSet.next()) {
                        throw new SQLException("Incompatible PlayPro column " + table + "." + expected.name
                                + ": expected " + expected.type + ", found " + actualType);
                    }
                }
            }
        }
    }

    private static String tableEngine(Connection connection, String database, String table) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT engine FROM system.tables WHERE database=? AND name=? LIMIT 2")) {
            statement.setString(1, database);
            statement.setString(2, table);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                String engine = resultSet.getString(1);
                if (resultSet.next()) {
                    throw new SQLException("Ambiguous ClickHouse object " + database + "." + table);
                }
                return engine;
            }
        }
    }

    private static void requireTargetDatabaseEngine(Connection connection, MigrationOptions options) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT engine,toString(uuid) FROM system.databases WHERE name=? LIMIT 2")) {
            statement.setString(1, options.database);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new SQLException("Target database does not exist: " + options.database);
                }
                String engine = resultSet.getString(1);
                String uuid = resultSet.getString(2);
                if (resultSet.next()) {
                    throw new SQLException("Target database is ambiguous: " + options.database);
                }
                if ("00000000-0000-0000-0000-000000000000".equals(uuid)) {
                    throw new SQLException("Target database uses engine " + engine + " without persistent UUID-backed table identities. Use ENGINE = Atomic.");
                }
            }
        }
    }

    private static void requireTargetReady(Connection connection, MigrationOptions options) throws SQLException {
        if (!tableExists(connection, options.database, options.livePrefix + "storage_metadata")
                || !tableExists(connection, options.database, options.livePrefix + "event_data")) {
            throw new SQLException("Target PlayPro schema is missing physical tables");
        }

        long identityRows = count(connection, qualified(options.database, options.livePrefix + "storage_metadata"));
        if (identityRows != 1L) {
            throw new SQLException("Target storage metadata must contain exactly one row. Found " + identityRows);
        }

        long eventRows = count(connection, qualified(options.database, options.livePrefix + "event_data"));
        if (eventRows > 0L) {
            throw new SQLException("Target event_data already contains " + eventRows + " rows. Use a fresh target database.");
        }

        long writerRows = count(connection, qualified(options.database, options.livePrefix + "writer_registration"));
        if (writerRows > 0L) {
            throw new SQLException("Target writer_registration already contains " + writerRows + " rows. Stop official PlayPro before migrating.");
        }

        long highWaterRows = count(connection, qualified(options.database, options.livePrefix + "retention_high_water"));
        if (highWaterRows > 0L) {
            throw new SQLException("Target retention_high_water already contains " + highWaterRows + " rows. Use a fresh target schema.");
        }
    }

    private static void archiveSourceTables(Connection connection, MigrationOptions options) throws SQLException {
        String renameSql = ARCHIVE_TABLES.stream()
                .map(table -> qualified(options.database, options.livePrefix + table) + " TO " + qualified(options.database, options.archivePrefix + table))
                .collect(Collectors.joining(", "));
        try (Statement statement = connection.createStatement()) {
            statement.execute("RENAME TABLE " + renameSql);
        }
        for (String table : ARCHIVE_TABLES) {
            CoreProtect.getInstance().getSLF4JLogger().info("[PlayPro migration] Archived {} as {}",
                    qualified(options.database, options.livePrefix + table), qualified(options.database, options.archivePrefix + table));
        }
    }

    private static void archiveExistingPlayProTarget(Connection connection, MigrationOptions options) throws SQLException {
        String timestamp = String.valueOf(System.currentTimeMillis());
        List<String> renameTargets = new ArrayList<>();
        for (String table : PHYSICAL_TARGET_TABLES) {
            String liveTable = options.livePrefix + table;
            if (tableExists(connection, options.database, liveTable)) {
                renameTargets.add(qualified(options.database, liveTable) + " TO "
                        + qualified(options.database, options.livePrefix + "broken_playpro_" + timestamp + "_" + table));
            }
        }
        for (String table : ARCHIVE_TABLES) {
            String liveTable = options.livePrefix + table;
            String engine = tableEngine(connection, options.database, liveTable);
            if (engine == null) {
                continue;
            }
            if ("View".equals(engine)) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("DROP VIEW " + qualified(options.database, liveTable));
                }
            }
            else {
                renameTargets.add(qualified(options.database, liveTable) + " TO "
                        + qualified(options.database, options.livePrefix + "broken_playpro_" + timestamp + "_" + table));
            }
        }
        if (!renameTargets.isEmpty()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("RENAME TABLE " + String.join(", ", renameTargets));
            }
            CoreProtect.getInstance().getSLF4JLogger().info("[PlayPro migration] Archived existing PlayPro target objects with marker broken_playpro_{}.", timestamp);
        }
    }

    private static boolean tableExists(Connection connection, String database, String table) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT 1 FROM system.tables WHERE database=? AND name=? LIMIT 1")) {
            statement.setString(1, database);
            statement.setString(2, table);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    private static long count(Connection connection, String table) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery("SELECT count() FROM " + table)) {
            if (!resultSet.next()) {
                throw new SQLException("ClickHouse did not return a count for " + table);
            }
            return resultSet.getLong(1);
        }
    }

    private static void runMigrationSql(Connection connection, MigrationOptions options, CommandSender sender) throws Exception {
        String sql = loadMigrationSql(options);
        int index = 0;
        for (String rawStatement : splitStatements(sql)) {
            String statementSql = stripLineComments(rawStatement).trim();
            if (statementSql.isEmpty()) {
                continue;
            }
            index++;
            try (Statement statement = connection.createStatement()) {
                boolean hasResultSet = statement.execute(statementSql);
                if (hasResultSet) {
                    reportResultSet(sender, statement.getResultSet());
                }
                else {
                    CoreProtect.getInstance().getSLF4JLogger().info("PlayPro migration statement {} completed.", index);
                }
            }
            catch (Exception exception) {
                CoreProtect.getInstance().getSLF4JLogger().error("[PlayPro migration debug] Statement {} failed: {}", index, exception.getMessage(), exception);
                debugSql("failed migration statement " + index, statementSql);
                throw exception;
            }
        }
    }

    private static void verifyMigratedRows(Connection connection, MigrationOptions options) throws SQLException {
        String events = qualified(options.database, options.livePrefix + "event_data");
        for (String family : MIGRATED_FAMILIES) {
            long sourceRows = countSourceRows(connection, options, family);
            long targetRows;
            try (PreparedStatement statement = connection.prepareStatement("SELECT count() FROM " + events + " WHERE family=?")) {
                statement.setString(1, family);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        throw new SQLException("ClickHouse did not return a target count for family " + family);
                    }
                    targetRows = resultSet.getLong(1);
                }
            }
            if (sourceRows != targetRows) {
                throw new SQLException("PlayPro migration row mismatch for " + family + ": source=" + sourceRows + ", target=" + targetRows);
            }
        }

        verifyStrictRowIds(connection, events);
        warnHistoricalDuplicateRowIds(connection, events);
    }

    private static void verifyStrictRowIds(Connection connection, String events) throws SQLException {
        String familyList = STRICT_ROWID_FAMILIES.stream().map(value -> "'" + value + "'").collect(Collectors.joining(","));
        String duplicateSql = "SELECT family,count() AS keys,sum(rows) AS rows FROM "
                + "(SELECT family,rowid,count() AS rows FROM " + events + " WHERE family IN(" + familyList
                + ") GROUP BY family,rowid HAVING count()>1) GROUP BY family ORDER BY family";
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(duplicateSql)) {
            List<String> duplicates = new ArrayList<>();
            while (resultSet.next()) {
                duplicates.add(resultSet.getString("family") + " duplicate_keys=" + resultSet.getLong("keys") + " rows=" + resultSet.getLong("rows"));
            }
            if (!duplicates.isEmpty()) {
                throw new SQLException("PlayPro migration produced duplicate rowids in strict families: " + String.join(", ", duplicates));
            }
        }
    }

    private static void warnHistoricalDuplicateRowIds(Connection connection, String events) throws SQLException {
        String familyList = HISTORICAL_DUPLICATE_ROWID_FAMILIES.stream().map(value -> "'" + value + "'").collect(Collectors.joining(","));
        String duplicateSql = "SELECT family,count() AS keys,sum(rows) AS rows FROM "
                + "(SELECT family,rowid,count() AS rows FROM " + events + " WHERE family IN(" + familyList
                + ") GROUP BY family,rowid HAVING count()>1) GROUP BY family ORDER BY family";
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(duplicateSql)) {
            List<String> duplicates = new ArrayList<>();
            while (resultSet.next()) {
                duplicates.add(resultSet.getString("family") + " duplicate_keys=" + resultSet.getLong("keys") + " rows=" + resultSet.getLong("rows"));
            }
            if (!duplicates.isEmpty()) {
                CoreProtect.getInstance().getSLF4JLogger().warn("[PlayPro migration] Preserved non-critical historical duplicate rowids: {}", String.join(", ", duplicates));
            }
        }
    }

    private static long countSourceRows(Connection connection, MigrationOptions options, String family) throws SQLException {
        String source = qualified(options.sourceDatabase, options.sourcePrefix + family);
        String expression = ID_ROWID_SOURCE_FAMILIES.contains(family) ? "uniqExact(id)" : "count()";
        String finalModifier = FINAL_SOURCE_FAMILIES.contains(family) ? " FINAL" : "";
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery("SELECT " + expression + " FROM " + source + finalModifier)) {
            if (!resultSet.next()) {
                throw new SQLException("ClickHouse did not return a source count for family " + family);
            }
            return resultSet.getLong(1);
        }
    }

    private static void reconcileUsernameLogRows(Connection connection, MigrationOptions options, CommandSender sender) throws SQLException {
        String source = qualified(options.sourceDatabase, options.sourcePrefix + "username_log");
        String events = qualified(options.database, options.livePrefix + "event_data");
        String highWater = qualified(options.database, options.livePrefix + "retention_high_water");
        long physicalRows;
        long logicalKeys;
        String sourceDiagnostics = "SELECT count(),uniqExact(tuple(time,rowid)) FROM " + source;
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(sourceDiagnostics)) {
            if (!resultSet.next()) {
                throw new SQLException("ClickHouse did not return username_log duplicate diagnostics");
            }
            physicalRows = resultSet.getLong(1);
            logicalKeys = resultSet.getLong(2);
        }
        if (physicalRows == logicalKeys) {
            return;
        }

        long targetRows;
        long targetKeys;
        long nonMigrationRows;
        String targetDiagnostics = "SELECT count(),uniqExact(tuple(time,rowid)),countIf(producer_sequence!=180) FROM "
                + events + " WHERE family='username_log'";
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(targetDiagnostics)) {
            if (!resultSet.next()) {
                throw new SQLException("ClickHouse did not return migrated username_log diagnostics");
            }
            targetRows = resultSet.getLong(1);
            targetKeys = resultSet.getLong(2);
            nonMigrationRows = resultSet.getLong(3);
        }
        if (targetRows != physicalRows || targetKeys != physicalRows) {
            if (nonMigrationRows > 0) {
                throw new SQLException("Unexpected migrated username_log state: source=" + physicalRows
                        + ", source_keys=" + logicalKeys + ", target=" + targetRows + ", target_keys=" + targetKeys
                        + ", non_migration_rows=" + nonMigrationRows);
            }
            ok(sender, "Remapping " + (physicalRows - logicalKeys) + " conflicting username history rows to unique PlayPro rowids.");
            try (Statement statement = connection.createStatement()) {
                if (targetRows > 0) {
                    statement.execute("ALTER TABLE " + events + " DELETE WHERE family='username_log' SETTINGS mutations_sync=2");
                }
                statement.execute(usernameLogInsertSql(options));
            }

            try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(targetDiagnostics)) {
                if (!resultSet.next() || resultSet.getLong(1) != physicalRows || resultSet.getLong(2) != physicalRows
                        || resultSet.getLong(3) != 0) {
                    throw new SQLException("Unable to preserve every username_log row with a unique PlayPro rowid");
                }
            }
        }

        String highWaterSql = "INSERT INTO " + highWater
                + " (dataset_id,producer_id,producer_sequence,family,rowid,recorded_at) "
                + "SELECT identity.dataset_id,identity.producer_id,1001,'username_log',marks.rowid,now64(3,'UTC') "
                + "FROM (SELECT max(rowid) AS rowid FROM " + events + " WHERE family='username_log') AS marks "
                + "CROSS JOIN (SELECT any(dataset_id) AS dataset_id,any(producer_id) AS producer_id FROM "
                + qualified(options.database, options.livePrefix + "storage_metadata") + ") AS identity WHERE marks.rowid>"
                + "(SELECT ifNull(max(rowid),0) FROM " + highWater + " WHERE family='username_log')";
        try (Statement statement = connection.createStatement()) {
            statement.execute(highWaterSql);
        }
        ok(sender, "Preserved all " + physicalRows + " username history rows using unique PlayPro rowids.");
    }

    private static String usernameLogInsertSql(MigrationOptions options) {
        String source = qualified(options.sourceDatabase, options.sourcePrefix + "username_log");
        String events = qualified(options.database, options.livePrefix + "event_data");
        String storage = qualified(options.database, options.livePrefix + "storage_metadata");
        return usernameLogInsertSql(source, events, storage);
    }

    static String usernameLogInsertSql(String source, String events, String storage) {
        return "INSERT INTO " + events
                + " (dataset_id,producer_id,producer_sequence,batch_id,batch_ordinal,family,rowid,time,uuid,user_name) "
                + "SELECT identity.dataset_id,identity.producer_id,180,toUUID('00000000-0000-0000-0000-000000000180'),0,'username_log',"
                + "if(duplicate_ordinal=1,source_rowid,max_source_rowid+source_ordinal),time,"
                + "if(toString(uuid)='00000000-0000-0000-0000-000000000000','',toString(uuid)),user "
                + "FROM (SELECT rowid AS source_rowid,time,uuid,user,"
                + "row_number() OVER (PARTITION BY time,rowid ORDER BY toString(uuid),user) AS duplicate_ordinal,"
                + "row_number() OVER (ORDER BY time,rowid,toString(uuid),user) AS source_ordinal,"
                + "max(rowid) OVER () AS max_source_rowid FROM " + source + ") AS source_rows "
                + "CROSS JOIN (SELECT any(dataset_id) AS dataset_id,any(producer_id) AS producer_id FROM " + storage + ") AS identity";
    }

    static void verifyOfficialLookupShapes(Connection connection, String database, String prefix) throws SQLException {
        String block = qualified(database, prefix + "block");
        String entitySpawn = qualified(database, prefix + "entity_spawn");
        String lookupUnion = officialRawLookupUnionSql(database, prefix);
        try {
            verifyOptionalType(connection,
                    "SELECT toTypeName(metadata) FROM (" + lookupUnion + ") LIMIT 1",
                    "Array(Int8)", "raw lookup metadata union");
            verifyOfficialByteColumn(connection,
                    "SELECT metadata FROM (" + lookupUnion + ") LIMIT 1",
                    "metadata", "raw lookup metadata union");
            verifyOptionalType(connection,
                    "SELECT toTypeName(blockdata) FROM " + block + " LIMIT 1",
                    "Array(Int8)", "block blockdata lookup");
            verifyOfficialByteColumn(connection,
                    "SELECT meta,blockdata FROM " + block + " LIMIT 1",
                    "meta", "block meta lookup");
            verifyOfficialByteColumn(connection,
                    "SELECT meta,blockdata FROM " + block + " LIMIT 1",
                    "blockdata", "block blockdata lookup");
            verifyOptionalType(connection,
                    "SELECT toTypeName(data) FROM " + entitySpawn + " LIMIT 1",
                    "Array(Int8)", "entity spawn data lookup");
            verifyOfficialByteColumn(connection,
                    "SELECT data FROM " + entitySpawn + " LIMIT 1",
                    "data", "entity spawn data lookup");
        }
        catch (SQLException exception) {
            dumpOfficialLookupDebug(connection, database, prefix, lookupUnion, exception);
            throw exception;
        }
    }

    private static void verifyOptionalType(Connection connection, String sql, String expectedType, String label) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(sql)) {
            if (!resultSet.next()) {
                return;
            }
            String actualType = resultSet.getString(1);
            if (!expectedType.equals(actualType)) {
                throw new SQLException("Official PlayPro " + label + " has incompatible type: expected " + expectedType + ", found " + actualType);
            }
        }
    }

    private static void verifyOfficialByteColumn(Connection connection, String sql, String column, String label) throws SQLException {
        String quotedColumn = quote(column);
        String validationSql = "SELECT toTypeName(" + quotedColumn + "),"
                + "if(empty(" + quotedColumn + "),1,toUInt8(arrayElement(" + quotedColumn + ",1)=toInt8(0))) "
                + "FROM (" + sql + ") LIMIT 1";
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(validationSql)) {
            if (!resultSet.next()) {
                return;
            }
            String columnType = resultSet.getString(1);
            if (!"Array(Int8)".equals(columnType)) {
                throw new SQLException("Official PlayPro " + label + " is not readable as ClickHouse binary: expected Array(Int8) for "
                        + column + ", found " + columnType);
            }
            if (resultSet.getInt(2) != 1) {
                throw new SQLException("Official PlayPro " + label + " has invalid ClickHouse binary presence marker");
            }
        }
    }

    private static String officialRawLookupUnionSql(String database, String prefix) {
        return officialRawLookupBranches(database, prefix).stream().map(LookupDebugBranch::query).collect(Collectors.joining(" UNION ALL "));
    }

    private static List<LookupDebugBranch> officialRawLookupBranches(String database, String prefix) {
        String block = qualified(database, prefix + "block");
        String container = qualified(database, prefix + "container");
        String entityContainer = qualified(database, prefix + "entity_container");
        String item = qualified(database, prefix + "item");
        String entityInteraction = qualified(database, prefix + "entity_interaction");
        return List.of(
                new LookupDebugBranch("block", lookupBranch("'0' AS tbl,rowid AS id,time,`user`,wid,x,y,z,type,meta AS metadata,data,-1 AS amount,action,rolled_back,0 AS entity_spawn_rowid", block)),
                new LookupDebugBranch("container", lookupBranch("'1' AS tbl,rowid AS id,time,`user`,wid,x,y,z,type,metadata,data,amount,action,rolled_back,0 AS entity_spawn_rowid", container)),
                new LookupDebugBranch("entity_container", lookupBranch("'3' AS tbl,rowid AS id,time,`user`,wid,x,y,z,type,metadata,data,amount,action,rolled_back,entity_spawn_rowid", entityContainer)),
                new LookupDebugBranch("item", lookupBranch("'2' AS tbl,rowid AS id,time,`user`,wid,x,y,z,type,data AS metadata,0 AS data,amount,action,rolled_back,0 AS entity_spawn_rowid", item)),
                new LookupDebugBranch("entity_interaction", lookupBranch("'4' AS tbl,rowid AS id,time,`user`,wid,x,y,z,type,metadata,action AS data,-1 AS amount,4 AS action,rolled_back,entity_spawn_rowid", entityInteraction)));
    }

    private static String lookupBranch(String projection, String table) {
        return "SELECT * FROM (SELECT " + projection + " FROM " + table + " LIMIT 1)";
    }

    private static void dumpOfficialLookupDebug(Connection connection, String database, String prefix, String lookupUnion, SQLException exception) {
        CoreProtect.getInstance().getSLF4JLogger().warn("[PlayPro migration debug] Official lookup verification failed: {}", exception.getMessage(), exception);
        debugSql("official raw lookup union", lookupUnion);
        String tableList = List.of("block", "container", "entity_container", "item", "entity_interaction", "entity_spawn").stream()
                .map(table -> sqlString(prefix + table))
                .collect(Collectors.joining(","));
        debugQuery(connection, "view binary columns",
                "SELECT `table`,name,type FROM system.columns WHERE database=" + sqlString(database)
                        + " AND `table` IN (" + tableList + ")"
                        + " AND name IN ('meta','metadata','data','blockdata','action','type','amount','entity_spawn_rowid') ORDER BY `table`,position",
                200);
        debugQuery(connection, "event_data payload column formats",
                "SELECT family,"
                        + "multiIf(isNull(meta),'NULL',startsWith(hex(meta),'ACED'),'JAVA',startsWith(meta,'{'),'JSON_OBJECT',startsWith(meta,'['),'JSON_ARRAY',empty(meta),'EMPTY','OTHER') AS meta_format,"
                        + "multiIf(isNull(metadata),'NULL',startsWith(hex(metadata),'ACED'),'JAVA',startsWith(metadata,'{'),'JSON_OBJECT',startsWith(metadata,'['),'JSON_ARRAY',empty(metadata),'EMPTY','OTHER') AS metadata_format,"
                        + "multiIf(isNull(payload),'NULL',startsWith(hex(payload),'ACED'),'JAVA',startsWith(payload,'{'),'JSON_OBJECT',startsWith(payload,'['),'JSON_ARRAY',empty(payload),'EMPTY','OTHER') AS payload_format,"
                        + "count() AS rows FROM " + qualified(database, prefix + "event_data")
                        + " FINAL WHERE family IN ('block','container','entity_container','item','entity_interaction','entity','entity_spawn')"
                        + " GROUP BY family,meta_format,metadata_format,payload_format ORDER BY family,rows DESC",
                200);
        for (LookupDebugBranch branch : officialRawLookupBranches(database, prefix)) {
            debugSql("official raw lookup branch " + branch.label, branch.query);
            debugQuery(connection, "official raw lookup branch types " + branch.label,
                    "SELECT " + typeProjection("tbl", "id", "time", "user", "wid", "x", "y", "z", "type", "metadata", "data", "amount", "action", "rolled_back", "entity_spawn_rowid")
                            + " FROM (" + branch.query + ") LIMIT 1",
                    5);
            debugQuery(connection, "official raw lookup branch samples " + branch.label,
                    "SELECT tbl,id,type,toTypeName(metadata) AS metadata_type,toTypeName(data) AS data_type,toTypeName(action) AS action_type,left(toString(metadata),160) AS metadata_sample"
                            + " FROM (" + branch.query + ") LIMIT 3",
                    5);
        }
        debugQuery(connection, "official raw lookup union column types",
                "SELECT " + typeProjection("tbl", "id", "time", "user", "wid", "x", "y", "z", "type", "metadata", "data", "amount", "action", "rolled_back", "entity_spawn_rowid")
                        + " FROM (" + lookupUnion + ") LIMIT 1",
                5);
        debugQuery(connection, "official raw lookup union grouped metadata types",
                "SELECT tbl,toTypeName(metadata) AS metadata_type,toTypeName(data) AS data_type,toTypeName(action) AS action_type,count() AS rows"
                        + " FROM (" + lookupUnion + ") GROUP BY tbl,metadata_type,data_type,action_type ORDER BY tbl,metadata_type,data_type,action_type",
                100);
    }

    private static String typeProjection(String... columns) {
        return java.util.Arrays.stream(columns)
                .map(column -> "toTypeName(" + quote(column) + ") AS " + quote(column + "_type"))
                .collect(Collectors.joining(","));
    }

    private static void debugQuery(Connection connection, String label, String sql, int maxRows) {
        debugSql(label, sql);
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(sql)) {
            int columns = resultSet.getMetaData().getColumnCount();
            int row = 0;
            while (resultSet.next() && row < maxRows) {
                row++;
                List<String> values = new ArrayList<>(columns);
                for (int i = 1; i <= columns; i++) {
                    values.add(resultSet.getMetaData().getColumnLabel(i) + "=" + String.valueOf(resultSet.getObject(i)));
                }
                CoreProtect.getInstance().getSLF4JLogger().warn("[PlayPro migration debug] {} row {}: {}", label, row, String.join(" | ", values));
            }
            if (row == 0) {
                CoreProtect.getInstance().getSLF4JLogger().warn("[PlayPro migration debug] {} returned no rows.", label);
            }
        }
        catch (SQLException debugException) {
            CoreProtect.getInstance().getSLF4JLogger().warn("[PlayPro migration debug] {} failed: {}", label, debugException.getMessage(), debugException);
        }
    }

    private static void debugSql(String label, String sql) {
        int chunkSize = 3000;
        if (sql.length() <= chunkSize) {
            CoreProtect.getInstance().getSLF4JLogger().warn("[PlayPro migration debug] {} SQL: {}", label, sql);
            return;
        }
        int part = 1;
        for (int offset = 0; offset < sql.length(); offset += chunkSize) {
            int end = Math.min(sql.length(), offset + chunkSize);
            CoreProtect.getInstance().getSLF4JLogger().warn("[PlayPro migration debug] {} SQL part {}: {}", label, part++, sql.substring(offset, end));
        }
    }

    private static void reportResultSet(CommandSender sender, ResultSet resultSet) throws SQLException {
        if (resultSet == null) {
            return;
        }
        int columns = resultSet.getMetaData().getColumnCount();
        int rows = 0;
        while (resultSet.next()) {
            rows++;
            List<String> values = new ArrayList<>(columns);
            for (int i = 1; i <= columns; i++) {
                values.add(String.valueOf(resultSet.getObject(i)));
            }
            String line = String.join(" | ", values);
            CoreProtect.getInstance().getSLF4JLogger().info("[PlayPro migration] {}", line);
            if (rows <= 25) {
                sender.sendMessage(Component.text(line, NamedTextColor.GRAY));
            }
        }
        resultSet.close();
    }

    private static String loadMigrationSql(MigrationOptions options) throws Exception {
        try (InputStream stream = PlayProMigrationCommand.class.getResourceAsStream(MIGRATION_RESOURCE)) {
            if (stream == null) {
                throw new IllegalStateException("Missing migration resource: " + MIGRATION_RESOURCE);
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String sql = reader.lines().collect(Collectors.joining("\n"));
                sql = sql.replace("__SOURCE_TABLE_PREFIX__", options.sourceDatabase + "." + options.sourcePrefix);
                sql = sql.replace("__TARGET_TABLE_PREFIX__", options.database + "." + options.livePrefix);
                sql = sql.replace("CREATE DATABASE IF NOT EXISTS __TARGET_DATABASE__ ENGINE = Atomic;", "");
                return sql;
            }
        }
    }

    private static List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<>();
        int start = 0;
        boolean quote = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'') {
                if (quote && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    i++;
                    continue;
                }
                quote = !quote;
            }
            else if (!quote && c == ';') {
                String statement = sql.substring(start, i).trim();
                if (!statement.isEmpty()) {
                    statements.add(statement);
                }
                start = i + 1;
            }
        }
        String tail = sql.substring(start).trim();
        if (!tail.isEmpty()) {
            statements.add(tail);
        }
        return statements;
    }

    private static String stripLineComments(String sql) {
        StringBuilder result = new StringBuilder(sql.length());
        for (String line : sql.split("\\R")) {
            String trimmed = line.trim();
            if (!trimmed.startsWith("--")) {
                result.append(line).append('\n');
            }
        }
        return result.toString();
    }

    private static List<String> targetSchema(MigrationOptions options) {
        String storage = qualified(options.database, options.livePrefix + "storage_metadata");
        String writer = qualified(options.database, options.livePrefix + "writer_registration");
        String highWater = qualified(options.database, options.livePrefix + "retention_high_water");
        String events = qualified(options.database, options.livePrefix + "event_data");

        return List.of(
                """
                CREATE TABLE IF NOT EXISTS %s (
                    dataset_id UUID CODEC(ZSTD(3)),
                    producer_id UUID CODEC(ZSTD(3)),
                    schema_version UInt32 CODEC(Delta, ZSTD(3)),
                    created_at DateTime64(3, 'UTC') CODEC(Delta, ZSTD(3))
                ) ENGINE = MergeTree
                ORDER BY tuple()
                SETTINGS fsync_after_insert=1,fsync_part_directory=1
                """.formatted(storage),
                """
                CREATE TABLE IF NOT EXISTS %s (
                    dataset_id UUID,
                    producer_id UUID,
                    writer_id UUID,
                    registration_order UInt64 DEFAULT generateSnowflakeID(),
                    registered_at DateTime64(3, 'UTC')
                ) ENGINE = MergeTree
                ORDER BY (registration_order,writer_id)
                SETTINGS fsync_after_insert=1,fsync_part_directory=1
                """.formatted(writer),
                """
                CREATE TABLE IF NOT EXISTS %s (
                    dataset_id UUID CODEC(ZSTD(3)),
                    producer_id UUID CODEC(ZSTD(3)),
                    producer_sequence UInt64 CODEC(Delta, ZSTD(3)),
                    family LowCardinality(String) CODEC(ZSTD(3)),
                    rowid UInt64 CODEC(Delta, ZSTD(3)),
                    recorded_at DateTime64(3, 'UTC') CODEC(Delta, ZSTD(3))
                ) ENGINE = MergeTree
                ORDER BY (dataset_id,family,producer_sequence,rowid)
                SETTINGS fsync_after_insert=1,fsync_part_directory=1,non_replicated_deduplication_window=1000
                """.formatted(highWater),
                eventDataTable(events),
                "INSERT INTO " + storage + " (dataset_id,producer_id,schema_version,created_at) "
                        + "SELECT generateUUIDv4(), generateUUIDv4(), 1, now64(3, 'UTC') WHERE (SELECT count() FROM " + storage + ") = 0");
    }

    private static String eventDataTable(String events) {
        return """
                CREATE TABLE IF NOT EXISTS %s (
                    dataset_id UUID CODEC(ZSTD(3)),
                    producer_id UUID CODEC(ZSTD(3)),
                    producer_sequence UInt64 CODEC(Delta, ZSTD(3)),
                    batch_id UUID CODEC(ZSTD(3)),
                    batch_ordinal UInt32 CODEC(Delta, ZSTD(3)),
                    family LowCardinality(String) CODEC(ZSTD(3)),
                    rowid UInt64 CODEC(Delta, ZSTD(3)),
                    time UInt32 CODEC(Delta, ZSTD(3)),
                    user_id Nullable(UInt32) CODEC(ZSTD(3)),
                    wid UInt32 CODEC(Delta, ZSTD(3)),
                    x Int32 CODEC(Delta, ZSTD(3)),
                    y Nullable(Int32) CODEC(ZSTD(3)),
                    z Int32 CODEC(Delta, ZSTD(3)),
                    type Nullable(UInt32) CODEC(ZSTD(3)),
                    data Nullable(Int64) CODEC(ZSTD(3)),
                    payload Nullable(String) CODEC(ZSTD(3)),
                    meta Nullable(String) CODEC(ZSTD(3)),
                    blockdata Nullable(String) CODEC(ZSTD(3)),
                    action Nullable(UInt8) CODEC(ZSTD(3)),
                    rolled_back Nullable(UInt8) CODEC(ZSTD(3)),
                    amount Nullable(Int32) CODEC(ZSTD(3)),
                    metadata Nullable(String) CODEC(ZSTD(3)),
                    entity_spawn_rowid Nullable(UInt64) CODEC(ZSTD(3)),
                    id Nullable(UInt32) CODEC(ZSTD(3)),
                    name Nullable(String) CODEC(ZSTD(3)),
                    text Nullable(String) CODEC(ZSTD(3)),
                    message Nullable(String) CODEC(ZSTD(3)),
                    status Nullable(UInt8) CODEC(ZSTD(3)),
                    database_lock_time Nullable(UInt32) CODEC(ZSTD(3)),
                    version Nullable(String) CODEC(ZSTD(3)),
                    block_rowid Nullable(UInt64) CODEC(ZSTD(3)),
                    kill_rowid Nullable(UInt64) CODEC(ZSTD(3)),
                    block_rowid_present Nullable(UInt8) CODEC(ZSTD(3)),
                    kill_rowid_present Nullable(UInt8) CODEC(ZSTD(3)),
                    uuid Nullable(String) CODEC(ZSTD(3)),
                    user_name Nullable(String) CODEC(ZSTD(3)),
                    current_wid Nullable(UInt32) CODEC(ZSTD(3)),
                    origin_x Nullable(Float64) CODEC(ZSTD(3)),
                    origin_y Nullable(Float64) CODEC(ZSTD(3)),
                    origin_z Nullable(Float64) CODEC(ZSTD(3)),
                    current_x Nullable(Float64) CODEC(ZSTD(3)),
                    current_y Nullable(Float64) CODEC(ZSTD(3)),
                    current_z Nullable(Float64) CODEC(ZSTD(3)),
                    yaw Nullable(Float32) CODEC(ZSTD(3)),
                    pitch Nullable(Float32) CODEC(ZSTD(3)),
                    entity_data Nullable(String) CODEC(ZSTD(3)),
                    entity_data_present Nullable(UInt8) CODEC(ZSTD(3)),
                    removed Nullable(UInt8) CODEC(ZSTD(3)),
                    color Nullable(UInt32) CODEC(ZSTD(3)),
                    color_secondary Nullable(UInt32) CODEC(ZSTD(3)),
                    sign_data Nullable(UInt8) CODEC(ZSTD(3)),
                    waxed Nullable(UInt8) CODEC(ZSTD(3)),
                    face Nullable(UInt8) CODEC(ZSTD(3)),
                    line_1 Nullable(String) CODEC(ZSTD(3)),
                    line_2 Nullable(String) CODEC(ZSTD(3)),
                    line_3 Nullable(String) CODEC(ZSTD(3)),
                    line_4 Nullable(String) CODEC(ZSTD(3)),
                    line_5 Nullable(String) CODEC(ZSTD(3)),
                    line_6 Nullable(String) CODEC(ZSTD(3)),
                    line_7 Nullable(String) CODEC(ZSTD(3)),
                    line_8 Nullable(String) CODEC(ZSTD(3)),
                    INDEX producer_sequence_idx producer_sequence TYPE minmax GRANULARITY 1,
                    INDEX rowid_idx rowid TYPE bloom_filter(0.01) GRANULARITY 1,
                    INDEX entity_uuid_idx uuid TYPE bloom_filter(0.01) GRANULARITY 1,
                    INDEX entity_kill_rowid_idx kill_rowid TYPE bloom_filter(0.01) GRANULARITY 1
                ) ENGINE = CoalescingMergeTree
                PARTITION BY if(family IN ('block','chat','command','container','entity_container','entity_interaction','item','entity','session','sign','skull'),toYYYYMM(toDateTime(time,'UTC')),0)
                ORDER BY (dataset_id,family,wid,x,z,if(family IN ('database_lock','user','version'),0,time),rowid)
                SETTINGS fsync_after_insert=1,fsync_part_directory=1,non_replicated_deduplication_window=1000
                """.formatted(events);
    }

    static List<String> compatibilityViewSql(String database, String prefix) {
        String events = qualified(database, prefix + "event_data");
        List<String> statements = new ArrayList<>();
        statements.add(view(database, prefix, events, "art_map", "e.rowid AS rowid,e.id AS id,e.name AS art"));
        statements.add(rollbackView(database, prefix, events, "block", "e.rowid AS rowid,e.time AS time,e.user_id AS `user`,e.wid AS wid,e.x AS x,e.y AS y,e.z AS z,e.type AS type,e.data AS data," + binary("e.meta", "meta") + "," + binary("e.blockdata", "blockdata") + ",e.action AS action"));
        statements.add(view(database, prefix, events, "chat", "e.rowid AS rowid,e.time AS time,e.user_id AS `user`,e.wid AS wid,e.x AS x,e.y AS y,e.z AS z,e.message AS message"));
        statements.add(view(database, prefix, events, "command", "e.rowid AS rowid,e.time AS time,e.user_id AS `user`,e.wid AS wid,e.x AS x,e.y AS y,e.z AS z,e.message AS message"));
        statements.add(rollbackView(database, prefix, events, "container", "e.rowid AS rowid,e.time AS time,e.user_id AS `user`,e.wid AS wid,e.x AS x,e.y AS y,e.z AS z,e.type AS type,e.data AS data,e.amount AS amount," + binary("e.metadata", "metadata") + ",e.action AS action"));
        statements.add(rollbackView(database, prefix, events, "entity_container", "e.rowid AS rowid,e.time AS time,e.user_id AS `user`,e.entity_spawn_rowid AS entity_spawn_rowid,e.wid AS wid,e.x AS x,e.y AS y,e.z AS z,e.type AS type,e.data AS data,e.amount AS amount," + binary("e.metadata", "metadata") + ",e.action AS action"));
        statements.add(view(database, prefix, events, "entity_interaction", "e.rowid AS rowid,e.time AS time,e.user_id AS `user`,e.entity_spawn_rowid AS entity_spawn_rowid,e.wid AS wid,e.x AS x,e.y AS y,e.z AS z,e.type AS type,e.action AS action," + binary("e.metadata", "metadata") + ",e.rolled_back AS rolled_back"));
        statements.add(rollbackView(database, prefix, events, "item", "e.rowid AS rowid,e.time AS time,e.user_id AS `user`,e.wid AS wid,e.x AS x,e.y AS y,e.z AS z,e.type AS type," + binary("e.payload", "data") + ",e.amount AS amount,e.action AS action"));
        statements.add(currentView(database, prefix, events, "database_lock", "e.rowid AS rowid,e.status AS status,e.database_lock_time AS time"));
        statements.add(view(database, prefix, events, "entity", "e.rowid AS rowid,e.time AS time," + binary("e.payload", "data")));
        statements.add(entitySpawnView(database, prefix, events));
        statements.add(view(database, prefix, events, "entity_map", "e.rowid AS rowid,e.id AS id,e.name AS entity"));
        statements.add(view(database, prefix, events, "material_map", "e.rowid AS rowid,e.id AS id,e.name AS material"));
        statements.add(view(database, prefix, events, "blockdata_map", "e.rowid AS rowid,e.id AS id,e.text AS data"));
        statements.add(view(database, prefix, events, "session", "e.rowid AS rowid,e.time AS time,e.user_id AS `user`,e.wid AS wid,e.x AS x,e.y AS y,e.z AS z,e.action AS action"));
        statements.add(view(database, prefix, events, "sign", "e.rowid AS rowid,e.time AS time,e.user_id AS `user`,e.wid AS wid,e.x AS x,e.y AS y,e.z AS z,e.action AS action,e.color AS color,e.color_secondary AS color_secondary,e.sign_data AS data,e.waxed AS waxed,e.face AS face,e.line_1 AS line_1,e.line_2 AS line_2,e.line_3 AS line_3,e.line_4 AS line_4,e.line_5 AS line_5,e.line_6 AS line_6,e.line_7 AS line_7,e.line_8 AS line_8"));
        statements.add(view(database, prefix, events, "skull", "e.rowid AS rowid,e.time AS time,e.name AS owner,e.text AS skin"));
        statements.add(currentView(database, prefix, events, "user", "e.rowid AS rowid,e.time AS time,e.user_name AS `user`,e.uuid AS uuid"));
        statements.add(view(database, prefix, events, "username_log", "e.rowid AS rowid,e.time AS time,e.uuid AS uuid,e.user_name AS `user`"));
        statements.add(currentView(database, prefix, events, "version", "e.rowid AS rowid,e.time AS time,e.version AS version"));
        statements.add(view(database, prefix, events, "world", "e.rowid AS rowid,e.id AS id,e.name AS world"));
        return statements;
    }

    private static String view(String database, String prefix, String events, String family, String projection) {
        return "CREATE OR REPLACE VIEW " + qualified(database, prefix + family)
                + " AS SELECT " + projection
                + " FROM " + events(events, family) + " AS e";
    }

    private static String currentView(String database, String prefix, String events, String family, String projection) {
        return "CREATE OR REPLACE VIEW " + qualified(database, prefix + family)
                + " AS SELECT " + projection
                + " FROM " + currentEvents(events, family) + " AS e";
    }

    private static String rollbackView(String database, String prefix, String events, String family, String projection) {
        return currentView(database, prefix, events, family, projection + ",e.rolled_back AS rolled_back");
    }

    private static String entitySpawnView(String database, String prefix, String events) {
        return "CREATE OR REPLACE VIEW " + qualified(database, prefix + "entity_spawn")
                + " AS SELECT e.rowid AS rowid,e.time AS time"
                + ",if(e.block_rowid_present=1,e.block_rowid,NULL) AS block_rowid"
                + ",if(e.kill_rowid_present=1,e.kill_rowid,NULL) AS kill_rowid"
                + ",e.uuid AS uuid,e.wid AS wid,e.current_wid AS current_wid"
                + ",e.origin_x AS origin_x,e.origin_y AS origin_y,e.origin_z AS origin_z"
                + ",e.current_x AS x,e.current_y AS y,e.current_z AS z"
                + ",e.yaw AS yaw,e.pitch AS pitch," + binary("if(e.entity_data_present=1,e.entity_data,NULL)", "data") + ",e.removed AS removed"
                + " FROM " + currentEvents(events, "entity_spawn") + " AS e";
    }

    private static String binary(String value, String alias) {
        String presentValue = "ifNull(" + value + ",'')";
        String bytes = "arrayMap(i -> reinterpretAsInt8(substring(" + presentValue + ",i,1)),range(1,length(" + presentValue + ")+1))";
        String expression = "if(isNull(" + value + "),CAST([], 'Array(Int8)'),arrayConcat([toInt8(0)]," + bytes + "))";
        return "CAST(" + expression + ", 'Array(Int8)') AS " + alias;
    }

    private static String events(String events, String family) {
        return "(SELECT * FROM " + events + " WHERE family='" + family + "')";
    }

    private static String currentEvents(String events, String family) {
        return "(SELECT * FROM " + events + " FINAL WHERE family='" + family + "')";
    }

    private static String qualified(String database, String table) {
        return quote(database) + "." + quote(table);
    }

    private static String similarTableHint(Connection connection, String database, String sourcePrefix) throws SQLException {
        List<String> tables = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT name FROM system.tables WHERE database=? AND (startsWith(name,?) OR startsWith(name,'co_')) ORDER BY name LIMIT 40")) {
            statement.setString(1, database);
            statement.setString(2, sourcePrefix);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    tables.add(resultSet.getString(1));
                }
            }
        }
        if (tables.isEmpty()) {
            return "";
        }
        return ". Found tables in " + database + ": " + String.join(", ", tables);
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private static String sqlString(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static void ok(CommandSender sender, String message) {
        sender.sendMessage(Component.text(message, NamedTextColor.GREEN));
        CoreProtect.getInstance().getSLF4JLogger().info("[PlayPro migration] {}", message);
    }

    private static void error(CommandSender sender, String message) {
        sender.sendMessage(Component.text(message, NamedTextColor.RED));
        CoreProtect.getInstance().getSLF4JLogger().warn("[PlayPro migration] {}", message);
    }

    private static void usage(CommandSender sender) {
        sender.sendMessage(Component.text("Usage: /co migrate-playpro [database:<current_database>] [prefix:co_] [archive-prefix:co_migrate_]", NamedTextColor.YELLOW));
        sender.sendMessage(Component.text("Failed/partial retry: /co migrate-playpro database:<current_database> prefix:co_ rebuild:true source-prefix:co_migrate_", NamedTextColor.YELLOW));
    }

    private record ExpectedColumn(String tableSuffix, String name, String type) {
    }

    private record LookupDebugBranch(String label, String query) {
    }

    private record MigrationOptions(String database, String livePrefix, String archivePrefix, String sourceDatabase, String sourcePrefix, boolean rebuild) {

        private static MigrationOptions parse(String[] commandArgs) {
            String database = ConfigHandler.database;
            String livePrefix = ConfigHandler.prefix;
            String archivePrefix = livePrefix + "migrate_";
            String sourceDatabase = null;
            String sourcePrefix = null;
            boolean sourceDatabaseConfigured = false;
            boolean sourcePrefixConfigured = false;
            boolean rebuild = false;

            for (int i = 1; i < commandArgs.length; i++) {
                String arg = commandArgs[i].trim();
                if (arg.isEmpty()) {
                    continue;
                }
                String[] split = arg.split(":", 2);
                if (split.length != 2) {
                    throw new IllegalArgumentException("Invalid argument: " + arg);
                }
                String key = split[0].toLowerCase(Locale.ROOT);
                String value = split[1];
                switch (key) {
                    case "database" -> database = value;
                    case "source-database" -> {
                        sourceDatabase = value;
                        sourceDatabaseConfigured = true;
                    }
                    case "prefix" -> {
                        livePrefix = value;
                        archivePrefix = value + "migrate_";
                    }
                    case "archive-prefix" -> archivePrefix = value;
                    case "source-prefix" -> {
                        sourcePrefix = value;
                        sourcePrefixConfigured = true;
                    }
                    case "rebuild" -> rebuild = Boolean.parseBoolean(value);
                    default -> throw new IllegalArgumentException("Unknown argument: " + key);
                }
            }

            if (!sourceDatabaseConfigured) {
                sourceDatabase = database;
            }
            if (!sourcePrefixConfigured) {
                sourcePrefix = archivePrefix;
            }
            validateIdentifier(database, "database");
            validateIdentifier(sourceDatabase, "source database");
            validatePrefix(livePrefix, "prefix");
            validatePrefix(archivePrefix, "archive prefix");
            validatePrefix(sourcePrefix, "source prefix");
            return new MigrationOptions(database, livePrefix, archivePrefix, sourceDatabase, sourcePrefix, rebuild);
        }

        private static void validateIdentifier(String value, String name) {
            if (value == null || !value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
                throw new IllegalArgumentException("Invalid " + name + ": " + value);
            }
        }

        private static void validatePrefix(String value, String name) {
            if (value == null) {
                throw new IllegalArgumentException("Invalid " + name + ": null");
            }
            if (!value.isEmpty() && !value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
                throw new IllegalArgumentException("Invalid " + name + ": " + value);
            }
        }
    }
}
