//package org.bg;
//
//import org.bg.avro.structures.AnotherUser;
//import org.bg.avro.structures.User;
//import org.bg.avro.structures.base.objects.Coordinate;
//import org.bg.avro.structures.base.objects.NullableTime;
//import org.bg.avro.structures.base.objects.Time;
//import org.joda.time.DateTime;
//
//public class Main {
//    public static void main(String[] args){
//        System.out.println("BGBG");
//
//        // Definition of an instance of user which uses base objects schemata
//        User myUserNullableTime = User.newBuilder()
//                .setUserCurrentTime(
//                        Time.newBuilder()
//                                .setTimeMillis(DateTime.now().getMillis())
//                                .build())
//                .setReportingSystemId(12)
//                .setUserPosition(Coordinate.newBuilder()
//                        .setAltitude(12345)
//                        .setLatitude(234555)
//                        .setLongitude(45666)
//                        .build())
//                .build();
//
//        // Definition of another user
//        User myUserWithPredictedTime = User.newBuilder()
//                .setUserCurrentTime(
//                        Time.newBuilder()
//                                .setTimeMillis(DateTime.now().getMillis())
//                                .build())
//                .setReportingSystemId(12)
//                .setUserPosition(Coordinate.newBuilder()
//                        .setAltitude(12345)
//                        .setLatitude(234555)
//                        .setLongitude(45666)
//                        .build())
//                .setUserPredictedTime(NullableTime.newBuilder()
//                        .setTimeMillis(Long.valueOf(23243443))
//                        .build())
//                .build();
//
//        // Definition of another kind of user generated from different schema, but has the same types.
//        AnotherUser anotherUser = AnotherUser.newBuilder()
//                .setReportingSystemId(myUserNullableTime.getReportingSystemId())
//                .setUserCurrentTime(myUserWithPredictedTime.getUserCurrentTime())
//                .setUserPosition(myUserNullableTime.getUserPosition())
//                .setUserPredictedTime(myUserWithPredictedTime.getUserPredictedTime())
//                .build();
//
//        System.out.println(anotherUser);
//    }
//}
