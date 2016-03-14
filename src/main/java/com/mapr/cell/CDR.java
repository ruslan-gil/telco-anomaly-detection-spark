package com.mapr.cell;

import org.apache.commons.beanutils.BeanUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.lang.reflect.InvocationTargetException;

public class CDR {
    enum State{
        CONNECT, RECONNECT, FINISHED;
    }

    private String callerId;
    private Double callStartTime;
    private String towerId;
    private Double duration;
    private State state;

    public CDR() {
    }

    public CDR(String id, double time) {
        callerId = id;
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
                '}';
    }
}