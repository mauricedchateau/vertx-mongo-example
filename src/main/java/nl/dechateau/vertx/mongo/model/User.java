package nl.dechateau.vertx.mongo.model;

public class User {
  public static class Name {
    private String firstName, lastName;

    public String getFirstName() {
      return firstName;
    }

    public void setFirstName(String firstName) {
      this.firstName = firstName;
    }

    public String getLastName() {
      return lastName;
    }

    public void setLastName(String lastName) {
      this.lastName = lastName;
    }
  }

  public enum Gender {
    MALE, FEMALE
  };

  private Name name;
  private Gender gender;

  public User(String firstName, String lastName, Gender gender) {
    name = new Name();
    name.setFirstName(firstName);
    name.setLastName(lastName);
    this.gender = gender;
  }

  public Name getName() {
    return name;
  }

  public Gender getGender() {
    return gender;
  }
}
