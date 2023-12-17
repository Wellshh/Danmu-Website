package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.dto.RegisterUserReq;
import io.sustc.service.DatabaseService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

@Service
@Slf4j
public class UserServiceImpl implements UserService {
    @Autowired
    private DataSource dataSource;
    private List<UserRecord> users;

    @Override
    public long register(RegisterUserReq req) throws SQLException {
        boolean can_register = true;
        if (req.getPassword() == null || req.getName() == null || req.getSex() == null) {
            can_register = false;
            return -1;
        }
        if (req.getBirthday() != null && req.getBirthday().matches("\\d{1,2}月\\d{1,2}日")) {//TODO:生日设置需要修改
            can_register = false;
            return -1;
        }

        String sql = "SELECT COUNT(*) FROM \"UserRecord\" WHERE Name = ? OR qq = ? OR Wechat = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
        ) {
            statement.setString(1, req.getName());
            statement.setString(2, req.getQq());
            statement.setString(3, req.getWechat());
            log.info("SQL: {}", statement);

            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            if (resultSet.getInt(1) > 0) {
                can_register = false;
                return -1;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        if (can_register) {
            UserRecord newUser = new UserRecord();
            long mid = generatedNewMid();
            newUser.setMid(mid);
            newUser.setName(req.getName());
            newUser.setSex(req.getSex().name());
            if (req.getBirthday() != null) {
                newUser.setBirthday(req.getBirthday());
            }
            newUser.setLevel((short) 0); // Assuming default level is 0
            newUser.setCoin(0); // Assuming default coin is 0
            if (req.getSign() != null) {
                newUser.setSign(req.getSign());
            }
            newUser.setPassword(req.getPassword());
            if (req.getQq() != null) {
                newUser.setQq(req.getQq());
            }
            if (req.getWechat() != null) {
                newUser.setWechat(req.getWechat());
            }
            newUser.setUser_is_Deleted(false);
            newUser.setIdentity(UserRecord.Identity.USER);

            String sql_insert = "INSERT INTO \"UserRecord\" (mid, name, sex, birthday, level, coin, sign, identity, password, qq, wechat, user_is_deleted) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement insertUser = connection.prepareStatement(sql_insert)
            ) {
                insertUser.setLong(1, newUser.getMid());
                insertUser.setString(2, newUser.getName());
                insertUser.setString(3, newUser.getSex());
                insertUser.setString(4, newUser.getBirthday());
                insertUser.setShort(5, newUser.getLevel());
                insertUser.setInt(6, newUser.getCoin());
                insertUser.setString(7, newUser.getSign());
                insertUser.setString(8, newUser.getIdentity().name());
                insertUser.setString(9, newUser.getPassword());
                insertUser.setString(10, newUser.getQq());
                insertUser.setString(11, newUser.getWechat());
                insertUser.setBoolean(12, newUser.isUser_is_Deleted());

                insertUser.executeUpdate();
                log.info("SQL: {}", insertUser);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            return mid;
        } else {
            return -1;
        }
    }

    private long generatedNewMid() {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("Select max(mid) from \"UserRecord\"");
        ) {
            log.info("SQL: {}", statement);

            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            System.out.println(resultSet.getLong(1) + 1);
            return resultSet.getLong(1) + 1;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long mid) {
        boolean can_delete = true;
        String sql_mid = "SELECT COUNT(*) FROM \"UserRecord\" WHERE mid = ?";
        String sql_qq_wechat = "SELECT ((SELECT mid FROM \"UserRecord\" WHERE qq = ?) INTERSECT (SELECT mid FROM \"UserRecord\" WHERE wechat = ?))";
        String sql_qq_wechat_existance = "SELECT COUNT(*) FROM \"UserRecord\" WHERE qq = ? OR wechat = ?";
        String sql_identity = "SELECT identity FROM \"UserRecord\" WHERE mid = ?";
        String delete_sql = "DELETE FROM \"UserRecord\" WHERE mid = ?";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql_mid);
             PreparedStatement preparedStatement1 = connection.prepareStatement(sql_qq_wechat);
             PreparedStatement preparedStatement2 = connection.prepareStatement(sql_qq_wechat_existance);
             PreparedStatement preparedStatement3 = connection.prepareStatement(sql_identity);
             PreparedStatement preparedStatement4 = connection.prepareStatement(sql_identity);
             Connection delete_connection = dataSource.getConnection();
             PreparedStatement delete_statement = delete_connection.prepareStatement(delete_sql);
        ) {
            preparedStatement.setLong(1, mid);
            preparedStatement2.setString(1, auth.getQq());
            preparedStatement2.setString(2, auth.getWechat());
            preparedStatement3.setLong(1, auth.getMid());
            preparedStatement4.setLong(1, mid);

            log.info("SQL: {}", preparedStatement);
            log.info("SQL: {}", preparedStatement1);
            log.info("SQL: {}", preparedStatement2);
            log.info("SQL: {}", preparedStatement3);
            log.info("SQL: {}", preparedStatement4);

            try (ResultSet resultSet_identity = preparedStatement3.executeQuery();
                 ResultSet resultSet_qq_wechat = preparedStatement2.executeQuery();
                 ResultSet resultSet_superuser = preparedStatement4.executeQuery();
                 ResultSet resultSet = preparedStatement.executeQuery();
            ) {
                resultSet_superuser.next();
                resultSet_qq_wechat.next();
                resultSet_identity.next();
                resultSet.next();

                if (resultSet.getInt(1) == 0) {
                    can_delete = false;
                    System.out.println("can't find user-to-be deleted");
                    return false;
                }

                String identity = resultSet_identity.getString(1);
                String superuser = resultSet_superuser.getString(1);

                if (identity.equalsIgnoreCase("User") && (auth.getMid() != mid)) {
                    can_delete = false;
                    System.out.println("not a superuser to delete");
                    return false;
                } else if (identity.equalsIgnoreCase("Superuser") && superuser.equalsIgnoreCase("Superuser") && (auth.getMid() != mid)) {
                    can_delete = false;
                    System.out.println("deleted account is a superuser");
                    return false;
                }

                if (auth.getQq() != null && auth.getWechat() != null) {
                    preparedStatement1.setString(1, auth.getQq());
                    preparedStatement1.setString(2, auth.getWechat());
                    try (ResultSet resultSet1 = preparedStatement1.executeQuery()) {
                        resultSet.next();
                        resultSet1.next();
                        if (resultSet1.wasNull()) {
                            can_delete = false;
                            System.out.println("qq and wechat don't match");
                            return false;
                        }
                    }
                } else if (resultSet.getInt(1) == 0 && (resultSet_qq_wechat.getInt(1) == 0 || (auth.getQq() == null && auth.getWechat() == null))) {
                    can_delete = false;
                    System.out.println("qq and wechat are invalid");
                    return false;
                }
            }

            delete_statement.setLong(1, mid);
            log.info("SQL: {}", delete_statement);
            delete_statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeMid) {
        return false;
    }

    @Override
    public UserInfoResp getUserInfo(long mid) {
        return null;
    }
}
