package jm.task.core.jdbc;

import jm.task.core.jdbc.service.UserService;
import jm.task.core.jdbc.service.UserServiceImpl;
import jm.task.core.jdbc.util.Util;


public class Main {
    public static void main(String[] args) {
        // реализуйте алгоритм здесь

        UserService userService = new UserServiceImpl();

        userService.createUsersTable();

        userService.saveUser("Vase","Krasnov", (byte) 34);
        userService.saveUser("Olga","Krasnova", (byte) 36);
        userService.saveUser("Dima","Krasnov", (byte) 38);
        userService.saveUser("Oleg","Krasnov", (byte) 33);
        userService.removeUserById(3);

        System.out.println(userService.getAllUsers());

        userService.cleanUsersTable();
        userService.dropUsersTable();

        Util.disconnect();
    }
}
