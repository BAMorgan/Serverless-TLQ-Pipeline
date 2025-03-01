/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

import java.util.HashMap;
import java.util.LinkedList;

public class Response extends saaf.Response {

    private String value;
    private LinkedList<HashMap<String, String>> records;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public LinkedList<HashMap<String, String>> getRecords() {
        return records;
    }

    @Override
    public String toString() {
        return "value=" + this.getValue() + ", records=" + this.records + super.toString();
    }

    
}

