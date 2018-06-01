package exercise49;

import java.io.Serializable;

public class Range implements Serializable {

    private String name, surname, range;

    public Range() {
    }

    public Range(String name, String surname, String range) {
        this.name = name;
        this.surname = surname;
        this.range = range;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public String getRange() {
        return range;
    }

    public void setRange(String range) {
        this.range = range;
    }

}
