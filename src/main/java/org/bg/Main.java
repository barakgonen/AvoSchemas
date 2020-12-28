package org.bg;

import org.bg.avro.structures.base.objects.Coordinate;
import org.bg.avro.structures.base.objects.Employee;
import org.bg.avro.structures.base.objects.NullableTime;
import org.bg.avro.structures.base.objects.Time;
import org.bg.avro.structures.objects.Manager;
import org.joda.time.DateTime;

public class Main {
    public static void main(String[] args){
        System.out.println("BGBG");
        Manager manager = Manager.newBuilder()
                .setEmployeeProp(Employee.newBuilder()
                        .setActive(true)
                        .setSalary(12323)
                        .setName("bgbg")
                        .build())
                .setHappy(true)
                .setNullableTime(NullableTime.newBuilder().setTimeMillis(DateTime.now().toDateTime().getMillis()).build())
                .setPosition(Coordinate.newBuilder().setAltitude(23).setLat(233).setLon(2331).build())
                .setMustAppearTimeField(Time.newBuilder().setTimeMllis(231111111).build())
                .build();
        System.out.println(manager);
    }
}
