package exercise35;

import scala.Serializable;

import java.sql.Timestamp;

public class DateFormat implements Serializable {

    private String _c1;

    public DateFormat(String _c1) {
        this._c1 = _c1;
    }

    public DateFormat(Timestamp _c1) {
        this._c1 = _c1.toString();
    }

    public String get_c1() {
        return _c1;
    }

    public void set_c1(String _c1) {
        this._c1 = _c1;
    }

}
