package org.example.demos;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/YYYY");
        Date date = new Date();
        System.out.println(dateFormat.format(date));

        String monthYear1 = LocalDate.now().withDayOfMonth(1).minusDays(1)
                .format(DateTimeFormatter.ofPattern("MMMYYYY"));

        String monthYear2 = LocalDate.now().withDayOfMonth(1).minusDays(1)
                .format(DateTimeFormatter.ofPattern("MMMyyyy"));

        System.out.println(monthYear1);
        System.out.println(monthYear2);
        System.out.println(LocalDate.now().format(DateTimeFormatter.ofPattern("MM/dd/YYYY")));

    }
}