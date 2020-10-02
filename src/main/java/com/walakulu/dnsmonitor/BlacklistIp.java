package com.walakulu.dnsmonitor;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Scanner;

public class BlacklistIp {
    private HashSet<String> set;

    public BlacklistIp() {
        this.set = new HashSet<>();
    }

    public void init_blacklist() throws FileNotFoundException {
        File myObj = new File("blacklist_ip.txt");
        Scanner myReader = new Scanner(myObj);
        while (myReader.hasNextLine()) {
            set.add(myReader.nextLine());
        }
        myReader.close();
    }

    public boolean search_ip(String ip) {
        return this.set.contains(ip);
    }
}
