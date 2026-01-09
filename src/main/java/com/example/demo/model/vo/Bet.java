package com.example.demo.model.vo;

import lombok.Data;

@Data
public class Bet {
    private String game;
    private long amount;
    private String contents;
    private double odds;
    private String title;
}
