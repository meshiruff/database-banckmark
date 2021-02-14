package database;

import java.util.Random;

public class Person {
    private int id;

    public String getFirstname() {
        return firstname;
    }

    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    public String getLastname() {
        return lastname;
    }

    public void setLastname(String lastname) {
        this.lastname = lastname;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getUniversity() {
        return university;
    }

    public void setUniversity(String university) {
        this.university = university;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getCredit_type() {
        return credit_type;
    }

    public void setCredit_type(String credit_type) {
        this.credit_type = credit_type;
    }

    public String getSecond_email() {
        return second_email;
    }

    public void setSecond_email(String second_email) {
        this.second_email = second_email;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getMusic_genre() {
        return music_genre;
    }

    public void setMusic_genre(String music_genre) {
        this.music_genre = music_genre;
    }

    public String getMother_name() {
        return mother_name;
    }

    public void setMother_name(String mother_name) {
        this.mother_name = mother_name;
    }

    public String getFather_name() {
        return father_name;
    }

    public void setFather_name(String father_name) {
        this.father_name = father_name;
    }

    private String firstname;
    private String lastname;
    private String email;
    private String university;
    private String company;
    private String credit_type;
    private String second_email;
    private int age;
    private String music_genre;
    private String mother_name;
    private String father_name;
    private String mother_email;
    private String father_email;
    private String country;
    private String address;
    private String shipping_address;
    private String mother_address;
    private String father_address;

    public int getRate() {
        return rate;
    }

    public int getScore() {
        return score;
    }

    private int rate;
    private int score;

    public Person(int id, String firstname, String lastname, String email, String university, String company, String credit_type, String second_email, int age, String music_genre, String mother_name, String father_name, String mother_email, String father_email, String country, String address, String shipping_address, String mother_address, String father_address, String business_address) {
        this.id = id;
        this.firstname = firstname;
        this.lastname = lastname;
        this.email = email;
        this.university = university;
        this.company = company;
        this.credit_type = credit_type;
        this.second_email = second_email;
        this.age = age;
        this.music_genre = music_genre;
        this.mother_name = mother_name;
        this.father_name = father_name;
        this.mother_email = mother_email;
        this.father_email = father_email;
        this.country = country;
        this.address = address;
        this.shipping_address = shipping_address;
        this.mother_address = mother_address;
        this.father_address = father_address;
        this.business_address = business_address;
        this.rate = id % 10;
        this.score = (id % 10) * 2;
    }

    public String getMother_email() {
        return mother_email;
    }

    public void setMother_email(String mother_email) {
        this.mother_email = mother_email;
    }

    public String getFather_email() {
        return father_email;
    }

    public void setFather_email(String father_email) {
        this.father_email = father_email;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getShipping_address() {
        return shipping_address;
    }

    public void setShipping_address(String shipping_address) {
        this.shipping_address = shipping_address;
    }

    public String getMother_address() {
        return mother_address;
    }

    public void setMother_address(String mother_address) {
        this.mother_address = mother_address;
    }

    public String getFather_address() {
        return father_address;
    }

    public void setFather_address(String father_address) {
        this.father_address = father_address;
    }

    public String getBusiness_address() {
        return business_address;
    }

    public void setBusiness_address(String business_address) {
        this.business_address = business_address;
    }

    private String business_address;



}


