package com.devshawn.kafka.gitops.cli;

import com.devshawn.kafka.gitops.MainCommand;
import com.devshawn.kafka.gitops.StateManager;
import com.devshawn.kafka.gitops.config.ManagerConfig;
import com.devshawn.kafka.gitops.exception.*;
import com.devshawn.kafka.gitops.service.ParserService;
import com.devshawn.kafka.gitops.util.LogUtil;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "account", description = "Create Confluent Cloud service accounts.")
public class AccountCommand implements Callable<Integer> {

    @CommandLine.Option(names = {"--check"}, description = "Check mode ( will output accounts to be created, but not actual")
    private boolean check = false;

    @CommandLine.ParentCommand
    private MainCommand parent;

    @Override
    public Integer call() {
        try {
            if (check) {
                System.out.println("Checking service accounts to be created...\n");
            } else {
                System.out.println("Creating service accounts...\n");
            }
            ParserService parserService = new ParserService(parent.getFile());
            StateManager stateManager = new StateManager(generateStateManagerConfig(), parserService);
            stateManager.createServiceAccounts(check);
            return 0;
        } catch (MissingConfigurationException | ConfluentCloudException ex) {
            LogUtil.printSimpleError(ex.getMessage());
        } catch (ValidationException ex) {
            LogUtil.printValidationResult(ex.getMessage(), false);
        } catch (KafkaExecutionException ex) {
            LogUtil.printKafkaExecutionError(ex);
        } catch (WritePlanOutputException ex) {
            LogUtil.printPlanOutputError(ex);
        }
        return 2;
    }

    private ManagerConfig generateStateManagerConfig() {
        return new ManagerConfig.Builder()
                .setVerboseRequested(parent.isVerboseRequested())
                .setDeleteDisabled(parent.isDeleteDisabled())
                .setIncludeUnchangedEnabled(false)
                .setStateFile(parent.getFile())
                .build();
    }
}
