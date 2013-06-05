package nl.dechateau.vertx.mongo;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class ExampleVerticle extends BusModBase {
  @Override
  public void start(final Future<Void> startResult) {
    super.start();
    container.logger().info("Starting Example verticle.");

    // Deploy the persistor module.
    container.deployModule(config.getString("PERSISTOR_MODULE_NAME"), config,
        new AsyncResultHandler<String>() {
          @Override
          public void handle(AsyncResult<String> result) {
            if (result.succeeded()) {
              container.logger().info("Persistor module deployed.");
            } else {
              container.logger().fatal("Problem deploying persistor module.", result.cause());
              throw new IllegalStateException("Persistor module not available.");
            }
            startResult.setResult(null);
          }
        });

    // Register a handler for incoming 'service' commands.
    vertx.eventBus().registerHandler(config.getString("EXAMPLE_VERTICLE_ADDRESS"),
        new Handler<Message<JsonObject>>() {
          @Override
          public void handle(final Message<JsonObject> cmdMsg) {
            String command = cmdMsg.body().getString("COMMAND");
            container.logger().info("Received command: " + command);

            final JsonObject actionDoc = new JsonObject();
            actionDoc.putString("collection", config.getString("COLLECTION_NAME"));
            switch (command) {
              case "CREATE_USER":
                handleCreateAction(cmdMsg, actionDoc);
                break;
              case "READ_USER":
                handleReadAction(cmdMsg, actionDoc);
                break;
              case "UPDATE_USER":
                handleUpdateAction(cmdMsg, actionDoc);
                break;
              case "DELETE_USER":
                handleDeleteAction(cmdMsg, actionDoc);
                break;
              default:
                container.logger().warn("Unsupported command: " + command);
                break;
            }
          }
        });

    container.logger().info("Example verticle started.");
  }

  private void handleCreateAction(final Message<JsonObject> cmdMsg, final JsonObject actionDoc) {
    actionDoc.putString("action", "save");
    actionDoc.putObject("document", cmdMsg.body().getObject("ARGUMENT"));
    eb.send(config.getString("address"), actionDoc, new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> reply) {
        if ("ok".equals(reply.body().getString("status"))) {
          container.logger().info("Added user to DB with _id=" + reply.body().getString("_id"));
          sendOK(cmdMsg);
        } else {
          String errMsg = "Failed to add user to DB; " + reply.body().getString("message");
          container.logger().info(reply.body().getString("status") + ": " + errMsg);
          sendError(cmdMsg, errMsg);
        }
      }
    });
    container.logger().info("Command for adding user sent.");
  }

  private void handleReadAction(final Message<JsonObject> cmdMsg, final JsonObject actionDoc) {
    actionDoc.putString("action", "find");
    actionDoc.putObject("matcher", cmdMsg.body().getObject("ARGUMENT"));
    eb.send(config.getString("address"), actionDoc, new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> reply) {
        if ("ok".equals(reply.body().getString("status"))) {
          container.logger().info(
              "Retrieved " + reply.body().getNumber("number") + " user(s) from DB for matcher "
                  + cmdMsg.body().getObject("ARGUMENT"));
          sendOK(cmdMsg);
        } else {
          String errMsg = "Failed to retrieve user from DB; " + reply.body().getString("message");
          container.logger().info(reply.body().getString("status") + ": " + errMsg);
          sendError(cmdMsg, errMsg);
        }
      }
    });
    container.logger().info("Command for reading user sent.");
  }

  private void handleUpdateAction(final Message<JsonObject> cmdMsg, final JsonObject actionDoc) {
    actionDoc.putString("action", "update");
    actionDoc.putObject("criteria", new JsonObject()); // Change ALL users.
    actionDoc.putObject("objNew", cmdMsg.body().getObject("ARGUMENT"));
    actionDoc.putBoolean("upsert", true);
    actionDoc.putBoolean("multi", false);
    eb.send(config.getString("address"), actionDoc, new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> reply) {
        if ("ok".equals(reply.body().getString("status"))) {
          container.logger().info(
              "Upserted " + reply.body().getNumber("number") + " user(s) in the DB with values "
                  + cmdMsg.body().getObject("ARGUMENT"));
          sendOK(cmdMsg);
        } else {
          String errMsg = "Failed to retrieve user from DB; " + reply.body().getString("message");
          container.logger().info(reply.body().getString("status") + ": " + errMsg);
          sendError(cmdMsg, errMsg);
        }
      }
    });
    container.logger().info("Command for updating user sent.");
  }

  private void handleDeleteAction(final Message<JsonObject> cmdMsg, final JsonObject actionDoc) {
    actionDoc.putString("action", "delete");
    actionDoc.putObject("matcher", cmdMsg.body().getObject("ARGUMENT"));
    eb.send(config.getString("address"), actionDoc, new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> reply) {
        if ("ok".equals(reply.body().getString("status"))) {
          container.logger().info(
              "Removed " + reply.body().getNumber("number") + " user(s) from DB for matcher "
                  + cmdMsg.body().getObject("ARGUMENT"));
          sendOK(cmdMsg);
        } else {
          String errMsg = "Failed to remove user from DB; " + reply.body().getString("message");
          container.logger().info(reply.body().getString("status") + ": " + errMsg);
          sendError(cmdMsg, errMsg);
        }
      }
    });
    container.logger().info("Command for deleting user sent.");
  }
}
