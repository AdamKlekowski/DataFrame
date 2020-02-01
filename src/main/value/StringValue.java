package main.value;

public class StringValue extends Value {
    public StringValue(String newValue) {
        innerValue = newValue;
    }
    public StringValue() {}


    @Override
    public String toString() {
        return innerValue.toString();
    }

    @Override
    public Value add(Value v) {
        return null;
    }

    @Override
    public Value sub(Value v) {
        return null;
    }

    @Override
    public Value mul(Value v) {
        return null;
    }

    @Override
    public Value div(Value v) {
        return null;
    }

    @Override
    public Value pow(Value v) {
        return null;
    }

    @Override
    public boolean eq(Value v) {
        return false;
    }

    @Override
    public boolean lte(Value v) {
        return ((String)innerValue).compareTo((String) v.innerValue) < 0;
    }

    @Override
    public boolean gte(Value v) {
        return ((String)innerValue).compareTo((String) v.innerValue) > 0;
    }

    @Override
    public boolean neq(Value v) {
        return false;
    }

    @Override
    public boolean equals(Value other) {
        return innerValue.equals(other.innerValue);
    }

    @Override
    public int hashCode() {
        return innerValue.hashCode();
    }

    @Override
    public StringValue create(String s) {
        StringValue newValue = new StringValue();
        newValue.innerValue = s;
        return newValue;
    }
}
