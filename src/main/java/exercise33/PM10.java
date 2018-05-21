package exercise33;

import scala.Serializable;

public class PM10 implements Serializable {

    private Double pm10;

    public PM10(double pm10) {
        this.pm10 = pm10;
    }

    public Double getPm10() {
        return pm10;
    }

    public void setPm10(Double pm10) {
        this.pm10 = pm10;
    }
}
