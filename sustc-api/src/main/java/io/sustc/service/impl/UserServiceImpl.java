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

public class UserServiceImpl implements UserService {
    @Autowired
    private DataSource dataSource;
    private List<UserRecord> users;

    @Override
    public long register(RegisterUserReq req) {
        long maxsize = 0;
        if (req.getPassword() == null || req.getName() == null || req.getSex() == null) {
            return -1;
        }
        if (req.getBirthday() != null && !req.getBirthday().matches("\\d{1,2}月\\d{1,2}日")) {
            return -1;
        }
        String sql = "Select count(*) from \"UserRecord\" where Name = ? or qq = ? or Wechat = ?";
        String sql_maxsize = "Select count(*) from \"UserRecord\"";
        //考虑两种方法：1. 在UserRecord类中添加一个静态的表，每次添加一个用户后就把信息存到表中； 2. 直接连接数据库进行比较
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             PreparedStatement statement1 = connection.prepareStatement(sql_maxsize);
        ) {
            statement.setString(1, req.getName());
            statement.setString(2, req.getQq());
            statement.setString(3, req.getWechat());

            ResultSet resultSet = statement.executeQuery();
            ResultSet resultSet1 = statement.executeQuery();
            resultSet.next();
            resultSet1.next();
            if (resultSet.getInt(1) > 0) {
                return -1;
            }
            maxsize = resultSet1.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
//        for (UserRecord existingUser : users) {
//            if (existingUser.getName().equals(req.getName()) ||
//                    (req.getQq() != null && !req.getQq().isEmpty() && existingUser.getQq() != null && existingUser.getQq().equals(req.getQq())) ||
//                    (req.getWechat() != null && !req.getWechat().isEmpty() && existingUser.getWechat() != null && existingUser.getWechat().equals(req.getWechat()))) {
//                return -1;
//            }
//        }
        UserRecord newUser = new UserRecord();
        long mid = generatedNewMid(maxsize);
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
//        newUser.setIdentity(UserRecord.Identity.USER); // Assuming default identity is USER
        newUser.setPassword(req.getPassword());
        if (req.getQq() != null && req.getWechat() != null) {
            newUser.setQq(req.getQq());
            newUser.setWechat(req.getWechat());
        }

        // Add the new user to the list
        //users.add(newUser);
        //TODO:是否需要将注册后的用户调用插入操作插入到相应数据库中
        return mid;
    }

    private long generatedNewMid(long maxsize) {
        return maxsize + 1;
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long mid) {
        String sql_mid = "Select count(*) from \"UserRecord\" where mid = ? ";
        String sql_qq_wechat = "Select((Select mid from \"UserRecord\" where qq = ?) INTERSECT (Select mid from \"UserRecord\" where wechat = ?));";
        String sql_qq_wechat_existance = "Select count(*) from \"UserRecord\" where qq = ? or wechat = ?";
        String sql_identity = "SELECT identity FROM \"UserRecord\" WHERE mid = ?";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql_mid);
             PreparedStatement preparedStatement1 = connection.prepareStatement(sql_qq_wechat);
             PreparedStatement preparedStatement2 = connection.prepareStatement(sql_qq_wechat_existance);
             PreparedStatement preparedStatement3 = connection.prepareStatement(sql_identity);
             PreparedStatement preparedStatement4 = connection.prepareStatement(sql_identity);//这是查询mid用户是否是超级用户
        ) {
            preparedStatement.setLong(1, mid);
            preparedStatement2.setString(1, auth.getQq());
            preparedStatement2.setString(2, auth.getWechat());
            preparedStatement3.setLong(1, auth.getMid());
            preparedStatement4.setLong(1, mid);
            ResultSet resultSet_identity = preparedStatement3.executeQuery();//查询发出删除请求的用户的身份
            ResultSet resultSet_qq_wechat = preparedStatement2.executeQuery();
            ResultSet resultSet_superuser = preparedStatement4.executeQuery();
            resultSet_superuser.next();
            resultSet_qq_wechat.next();
            resultSet_identity.next();
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.getInt(1) == 0) {
                return false;
            }
            if (resultSet_identity.getString(1).equalsIgnoreCase("User") && (auth.getMid() != mid)) {
                return false;
            } else if (resultSet_superuser.getString(1).equalsIgnoreCase("Superuser") && (auth.getMid() != mid)) {
                return false;
            }

            if (auth.getQq() != null && auth.getWechat() != null) {
                preparedStatement1.setString(1, auth.getQq());
                preparedStatement1.setString(1, auth.getWechat());
                ResultSet resultSet1 = preparedStatement1.executeQuery();
                resultSet.next();
                resultSet1.next();
                if (resultSet1.wasNull()) {
                    return false;
                }
            } else if (resultSet.getInt(1) == 0 && (resultSet_qq_wechat.getInt(1) == 0 || (auth.getQq() == null && auth.getWechat() == null))) {
                return false;
            }
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
