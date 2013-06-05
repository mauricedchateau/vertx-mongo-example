package nl.dechateau.vertx.mongo;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.vertx.testtools.VertxAssert.testComplete;
import nl.dechateau.vertx.mongo.model.User;
import nl.dechateau.vertx.mongo.model.User.Gender;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;

public class IntegrationTest extends TestVerticle {
  private static final String PERSISTOR_MODULE_NAME = "io.vertx~mod-mongo-persistor~2.0.0-beta2";
  private static final String PERSISTOR_MAIN_ADDRESS = "persistor.main.address";
  private static final String MONGO_HOST = "localhost";
  private static final int MONGO_PORT = 27017;
  private static final String DB_NAME = "vertx-example";
  private static final String COLLECTION_NAME = "users";

  public static final String EXAMPLE_VERTICLE_ADDRESS = "example.verticle.address";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private User user;
  private JsonObject createDoc;
  private JsonObject readDoc;
  private JsonObject updateDoc;
  private JsonObject deleteDoc;

  public IntegrationTest() throws Exception {
    // Clear the collection before starting the test.
    MongoClient mongoClient = new MongoClient(MONGO_HOST, MONGO_PORT);
    mongoClient.getDB(DB_NAME).getCollection(COLLECTION_NAME).drop();
    mongoClient.close();

    // Initialise the action documents.
    user = new User("John", "Doe", Gender.MALE);
    createDoc =
        new JsonObject().putString("COMMAND", "CREATE_USER").putObject("ARGUMENT",
            new JsonObject(MAPPER.writeValueAsString(user)));
    readDoc =
        new JsonObject().putString("COMMAND", "READ_USER").putObject("ARGUMENT",
            new JsonObject().putString("name.firstName", "John"));
    updateDoc =
        new JsonObject().putString("COMMAND", "UPDATE_USER").putObject(
            "ARGUMENT",
            new JsonObject().putObject("name", new JsonObject().putString("firstName", "Joe")
                .putString("lastName", "Done")));
    deleteDoc =
        new JsonObject().putString("COMMAND", "DELETE_USER").putObject("ARGUMENT",
            new JsonObject().putString("name.lastName", "Done"));
  }

  @Override
  public void start(final Future<Void> startResult) {
    container.logger().info("Starting Integrationtest verticle.");

    // Create configuration.
    JsonObject config = new JsonObject();
    config.putString("PERSISTOR_MODULE_NAME", PERSISTOR_MODULE_NAME);
    config.putString("address", PERSISTOR_MAIN_ADDRESS);
    config.putString("host", MONGO_HOST);
    config.putNumber("port", MONGO_PORT);
    config.putString("db_name", DB_NAME);
    config.putString("COLLECTION_NAME", COLLECTION_NAME);
    config.putString("EXAMPLE_VERTICLE_ADDRESS", EXAMPLE_VERTICLE_ADDRESS);

    container.logger().info("Deploying verticle under test.");
    container.deployVerticle(ExampleVerticle.class.getName(), config,
        new Handler<AsyncResult<String>>() {
          @Override
          public void handle(AsyncResult<String> event) {
            container.logger().info("Deployed verticle under test with ID: " + event.result());

            IntegrationTest.super.start();
            startResult.setResult(null);
          }
        });
  }

  @Test
  public void crudTest() throws Exception {
    container.logger().info("*** Starting CRUD test.");

    vertx.eventBus().send(EXAMPLE_VERTICLE_ADDRESS, createDoc, new CreateResponseHandler());
  }

  class CreateResponseHandler implements Handler<Message<JsonObject>> {
    @Override
    public void handle(Message<JsonObject> reply) {
      assertThat(reply.body().getString("status"), is(equalTo("ok")));

      container.logger().info("*** Create step complete.");

      vertx.eventBus().send(EXAMPLE_VERTICLE_ADDRESS, readDoc, new ReadResponseHandler());
    }
  }

  class ReadResponseHandler implements Handler<Message<JsonObject>> {
    @Override
    public void handle(Message<JsonObject> reply) {
      assertThat(reply.body().getString("status"), is(equalTo("ok")));

      container.logger().info("*** Read step complete.");

      vertx.eventBus().send(EXAMPLE_VERTICLE_ADDRESS, updateDoc, new UpdateResponseHandler());
    }
  }

  class UpdateResponseHandler implements Handler<Message<JsonObject>> {
    @Override
    public void handle(Message<JsonObject> reply) {
      assertThat(reply.body().getString("status"), is(equalTo("ok")));

      container.logger().info("*** Update step complete.");

      vertx.eventBus().send(EXAMPLE_VERTICLE_ADDRESS, deleteDoc, new DeleteResponseHandler());
    }
  }

  class DeleteResponseHandler implements Handler<Message<JsonObject>> {
    @Override
    public void handle(Message<JsonObject> reply) {
      assertThat(reply.body().getString("status"), is(equalTo("ok")));

      container.logger().info("*** Delete step complete.");

      container.logger().info("*** CRUD test ended.");
      testComplete();
    }
  }
}
