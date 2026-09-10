package net.coreprotect.consumer.process;

import java.sql.PreparedStatement;

import org.bukkit.Location;
import org.bukkit.Material;

import net.coreprotect.database.logger.PlayerInteractLogger;

class PlayerInteractionProcess {

    static void process(PreparedStatement preparedStmt, int batchCount, String user, Object object, Material type, String blockData) {
        if (object instanceof Location) {
            Location location = (Location) object;
            PlayerInteractLogger.log(preparedStmt, batchCount, user, location, type, blockData);
        }
    }
}
