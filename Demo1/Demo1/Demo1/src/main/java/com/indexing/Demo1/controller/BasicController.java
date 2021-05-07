package com.indexing.Demo1.controller;

import com.indexing.Demo1.ETag.EtagMap;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@RestController
public class BasicController {

    private Jedis jd;
    public BasicController(){
        jd=new Jedis();
    }

    @GetMapping(value = "/getAll")
    public @ResponseBody ResponseEntity<String> getAllJson (HttpServletRequest request, HttpServletResponse response) {
        Set<String> keys = jd.keys("*");
        Iterator<String> it = keys.iterator();
        String returnString="";
        while (it.hasNext()) {
            String s = it.next();
            String jsonString = jd.get(s);
            returnString = returnString+  "\n" + jsonString;
        }
        return ResponseEntity.status(HttpStatus.OK).body("List of json\n"+ returnString );
    }

    @GetMapping(path = "/plan/{key}")
    public @ResponseBody ResponseEntity<String> getKey(@PathVariable("key") String key , @RequestHeader HttpHeaders header, HttpServletRequest request)  {
        if(key != null){
            String etag = request.getHeader("If-None-Match");
            if(etag == ""){
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Enter the etag value for If-None match request header!!");
            }
            if (etag != null) {
                if (EtagMap.getEtags().containsKey(key)) {
                    if (!EtagMap.getEtags().get(key).equals(etag)) {
                        EtagMap.getEtags().put(key, etag);
                        String s = jd.get(key);
                        return ResponseEntity.status(HttpStatus.OK).eTag(etag).body(s);
                    }else{
                        return ResponseEntity.status(HttpStatus.NOT_MODIFIED).eTag(etag).body("Data has not been modified");
                    }
                }else{
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Key does not exist");
                }
            }else{
                //give error
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Enter the If-None Match request header!!");
            }

        }else{
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Enter the key!!");
        }

    }

    @PostMapping(value = "/plan")
    public @ResponseBody ResponseEntity<String> post(@RequestBody String data) throws ValidationException {
        try {
            InputStream ip = Thread.currentThread().getContextClassLoader().getResourceAsStream("data/use_case_schema.json");
           //JSON Tokener is used to parse the JSON source string
            JSONObject jsonData = new JSONObject(new JSONTokener(data));
            System.out.println(jsonData);
            JSONObject jsonSchema = new JSONObject(new JSONTokener(ip));
            //everit-org JSON schema validator for Java
            Schema schema = SchemaLoader.load(jsonSchema);
            schema.validate(jsonData);

            String key = jsonData.get("objectId").toString();

            if (!jd.exists(key)) {
                String etag = UUID.randomUUID().toString();
                EtagMap.getEtags().put(key, etag);
                jd.set(key, data);
                return ResponseEntity.status(HttpStatus.CREATED).eTag(etag).body("Data Saved successfully. Key for entry is: " + key);
            }else{
//                String etag = UUID.randomUUID().toString();
//                EtagMap.getEtags().put(key, etag);
                String etag = EtagMap.getEtags().get(key);
                return ResponseEntity.status(HttpStatus.ALREADY_REPORTED).eTag(etag).body("Key already present");
            }

        }catch(Exception e){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Validation Unsuccessful");

        }
    }

    @DeleteMapping(value = "/delete/{key}")
    public @ResponseBody
    ResponseEntity<String> delete(@PathVariable("key") String key) {
        Long delSuccess = jd.del(key);
        EtagMap.getEtags().remove(key);
        if (delSuccess == 1)
            return ResponseEntity.status(HttpStatus.OK).body("Key: " + key + " was deleted");
        else
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Deletion Unsuccessful");
    }

}
