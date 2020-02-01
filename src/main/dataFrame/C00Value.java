package main.dataFrame;

import main.value.Value;

final class C00Value extends Value {
    private final Integer place;
    private final Object hiddenValue;

    C00Value(Integer place, Object hiddenValue){
        this.place=place;
        this.hiddenValue=hiddenValue;
    }

    public Integer getPlace() {
        return place;
    }

    public Object getHiddenValue() {
        return hiddenValue;
    }

    @Override
    public String toString() {
        return null;
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
        return false;
    }

    @Override
    public boolean gte(Value v) {
        return false;
    }

    @Override
    public boolean neq(Value v) {
        return false;
    }

    @Override
    public boolean equals(Value other) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public Value create(String s) {
        return null;
    }
}
