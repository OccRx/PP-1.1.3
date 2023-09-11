package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    private final Connection connection = Util.getConnection();

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        String sql = "CREATE table IF NOT EXISTS users(" +
                "id bigserial primary key" +
                ",name varchar,lastName varchar, age int2 )";
        try (PreparedStatement pS = connection.prepareStatement(sql)) {
            connection.setAutoCommit(false);
            pS.execute();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }

    }

    public void dropUsersTable() {
        String sqlDrop = "DROP TABLE IF EXISTS users";
        try (PreparedStatement pr = connection.prepareStatement(sqlDrop)) {
            connection.setAutoCommit(false);
            pr.execute();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        String insert = "INSERT INTO users (name, lastName, age) VALUES (?,?,?)";
        try (PreparedStatement pr = connection.prepareStatement(insert)) {
            connection.setAutoCommit(false);
            pr.setString(1, name);
            pr.setString(2, lastName);
            pr.setByte(3, age);
            pr.execute();
            connection.commit();
            System.out.printf("User с именем – %s добавлен в базу данных \n", name);
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }

    }

    public void removeUserById(long id) {
        String deleteUser = "DELETE FROM users where id = ?";
        try (PreparedStatement pr = connection.prepareStatement(deleteUser)) {
            connection.setAutoCommit(false);
            pr.setLong(1, id);
            pr.execute();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public List<User> getAllUsers() {
        String getAllUser = "SELECT * FROM users";
        ArrayList<User> listUsers = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(getAllUser);
             ResultSet resultSet = ps.executeQuery()) {

            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong("id"));
                user.setName(resultSet.getString("name"));
                user.setLastName(resultSet.getString("lastName"));
                user.setAge(resultSet.getByte("age"));
                listUsers.add(user);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return listUsers;
    }

    public void cleanUsersTable() {
        String sql = "TRUNCATE TABLE users";
        try (PreparedStatement pS = connection.prepareStatement(sql)) {
            connection.setAutoCommit(false);
            pS.execute();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
