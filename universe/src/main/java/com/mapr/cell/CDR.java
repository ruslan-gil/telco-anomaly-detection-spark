package com.mapr.cell;

import org.apache.commons.beanutils.BeanUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.lang.reflect.InvocationTargetException;

public class CDR {
    enum State{
        CONNECT, RECONNECT, FINISHED, FAIL
    }

    private String callerId;
    private Double callStartTime;
    private String towerId;
    private Double duration;
    private State state;
    private double x;
    private double y;

    public CDR() {
    }

    public CDR(String id, double time, double x, double y) {
        callerId = id;
        this.x = x;
        this.y = y;
        callStartTime = time;
        state = State.CONNECT;
    }

    public CDR(String callerId, Double callStartTime, String towerId, Double duration, State state) {
        this.callerId = callerId;
        this.callStartTime = callStartTime;
        this.towerId = towerId;
        this.duration = duration;
        this.state = state;
    }


    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    public String getCallerId() {
        return callerId;
    }

    public void setCallerId(String callerId) {
        this.callerId = callerId;
    }

    public Double getCallStartTime() {
        return callStartTime;
    }

    public void setCallStartTime(Double callStartTime) {
        this.callStartTime = callStartTime;
    }

    public String getTowerId() {
        return towerId;
    }

    public void setTowerId(String towerId) {
        this.towerId = towerId;
    }

    public Double getDuration() {
        return duration;
    }

    public void setDuration(Double duration) {
        this.duration = duration;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }


    public void finishCDR(Double time) {
        this.state = State.FINISHED;
        this.duration = time - this.callStartTime;
    }

    public JSONObject toJSONObject(){
        try {
            return new JSONObject()
                    .put("callStartTime", callStartTime)
                    .put("callerId", callerId)
                    .put("duration", duration)
                    .put("towerId", towerId)
                    .put("x", x)
                    .put("y", y)
                    .put("state", state.name());
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    public CDR cloneCDR(){
        try {
            return (CDR) BeanUtils.cloneBean(this);
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException | NoSuchMethodException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String toString() {
        return "CDR{" +
                "callerId='" + callerId + '\'' +
                ", callStartTime=" + callStartTime +
                ", towerId='" + towerId + '\'' +
                ", duration=" + duration +
                ", state=" + state +
                ", x=" + x +
                ", y=" + y +
                '}';
    }
}