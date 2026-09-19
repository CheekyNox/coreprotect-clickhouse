package net.coreprotect.command;

import java.util.concurrent.CancellationException;

import org.bukkit.command.CommandSender;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.consumer.Consumer;
import net.coreprotect.language.Phrase;
import net.coreprotect.thread.NetworkHandler;
import net.coreprotect.utility.Chat;
import net.coreprotect.utility.Color;
import net.coreprotect.utility.ErrorReporter;
import net.coreprotect.utility.LookupThrottle;

public class ReloadCommand {
    protected static void runCommand(final CommandSender player, boolean permission, String[] args) {
        if (permission) {
            if (ConfigHandler.converterRunning) {
                Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.UPGRADE_IN_PROGRESS));
                return;
            }
            if (ConfigHandler.purgeRunning) {
                Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.PURGE_IN_PROGRESS));
                return;
            }
            if (!LookupThrottle.tryAcquire(player.getName(), 100)) {
                Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.DATABASE_BUSY));
                return;
            }

            class BasicThread implements Runnable {
                @Override
                public void run() {
                    try {
                        if (Consumer.isPaused) {
                            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.RELOAD_STARTED));
                        }
                        while (Consumer.isPaused) {
                            Thread.sleep(1);
                        }
                        Consumer.isPaused = true;

                        try {
                            ConfigHandler.performInitialization(false);
                        }
                        catch (CancellationException e) {
                            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.RELOAD_FAILED));
                            return;
                        }
                        Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.RELOAD_SUCCESS));

                        Thread networkHandler = new Thread(new NetworkHandler(false, false));
                        networkHandler.start();
                    }
                    catch (Exception e) {
                        ErrorReporter.report(e);
                    }
                    finally {
                        Consumer.isPaused = false;
                        LookupThrottle.release(player.getName());
                    }
                }
            }
            try {
                Runnable runnable = new BasicThread();
                Thread thread = new Thread(runnable);
                thread.start();
            }
            catch (RuntimeException | Error e) {
                LookupThrottle.release(player.getName());
                throw e;
            }
        }
        else {
            Chat.sendMessage(player, Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.NO_PERMISSION));
        }
    }
}
