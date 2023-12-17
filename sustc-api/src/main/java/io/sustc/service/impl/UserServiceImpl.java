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
import java.util.ArrayList;
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
    public boolean deleteAccount(AuthInfo auth, long mid) {//TODO:需要完成当用户被删除时，与其相关的所有信息都会被删除。
        boolean can_delete = true;
        String sql_mid = "SELECT COUNT(*) FROM \"UserRecord\" WHERE mid = ?";
        String sql_qq_wechat = "SELECT ((SELECT mid FROM \"UserRecord\" WHERE qq = ?) INTERSECT (SELECT mid FROM \"UserRecord\" WHERE wechat = ?))";
        String sql_qq_wechat_existance = "SELECT COUNT(*) FROM \"UserRecord\" WHERE qq = ? OR wechat = ?";
        String sql_identity = "SELECT identity FROM \"UserRecord\" WHERE mid = ?";
        String delete_sql = "DELETE FROM \"UserRecord\" WHERE mid = ?";

        try (Connection connection = dataSource.getConnection();
             Connection delete_connection = dataSource.getConnection();
        ) {
            PreparedStatement preparedStatement = connection.prepareStatement(sql_mid);
            PreparedStatement preparedStatement1 = connection.prepareStatement(sql_qq_wechat);
            PreparedStatement preparedStatement2 = connection.prepareStatement(sql_qq_wechat_existance);
            PreparedStatement preparedStatement3 = connection.prepareStatement(sql_identity);
            PreparedStatement preparedStatement4 = connection.prepareStatement(sql_identity);
            PreparedStatement delete_statement = delete_connection.prepareStatement(delete_sql);

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

            try (
                    ResultSet resultSet_identity = preparedStatement3.executeQuery();
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

            preparedStatement.close();
            preparedStatement1.close();
            preparedStatement2.close();
            preparedStatement3.close();
            preparedStatement4.close();
            delete_statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    public boolean CheckAuthoInfo(AuthInfo auth, long mid) {
        String sql_mid = "SELECT COUNT(*) FROM \"UserRecord\" WHERE mid = ?";
        String sql_qq_wechat = "SELECT ((SELECT mid FROM \"UserRecord\" WHERE qq = ?) INTERSECT (SELECT mid FROM \"UserRecord\" WHERE wechat = ?))";
        String sql_qq_wechat_existance = "SELECT COUNT(*) FROM \"UserRecord\" WHERE qq = ? OR wechat = ?";
        String delete_sql = "DELETE FROM \"UserRecord\" WHERE mid = ?";

        try (Connection connection = dataSource.getConnection();
             Connection delete_connection = dataSource.getConnection();
        ) {
            PreparedStatement preparedStatement = connection.prepareStatement(sql_mid);
            PreparedStatement preparedStatement1 = connection.prepareStatement(sql_qq_wechat);
            PreparedStatement preparedStatement2 = connection.prepareStatement(sql_qq_wechat_existance);
            PreparedStatement delete_statement = delete_connection.prepareStatement(delete_sql);

            preparedStatement.setLong(1, mid);
            preparedStatement2.setString(1, auth.getQq());
            preparedStatement2.setString(2, auth.getWechat());
            log.info("SQL: {}", preparedStatement);
            log.info("SQL: {}", preparedStatement1);
            log.info("SQL: {}", preparedStatement2);

            try (
                    ResultSet resultSet_qq_wechat = preparedStatement2.executeQuery();
                    ResultSet resultSet = preparedStatement.executeQuery();
            ) {
                resultSet_qq_wechat.next();
                resultSet.next();

                if (resultSet.getInt(1) == 0) {
                    System.out.println("can't find a user corresponding to the id");
                    return false;
                }

                if (auth.getQq() != null && auth.getWechat() != null) {
                    preparedStatement1.setString(1, auth.getQq());
                    preparedStatement1.setString(2, auth.getWechat());

                    try (ResultSet resultSet1 = preparedStatement1.executeQuery()) {
                        resultSet.next();
                        resultSet1.next();
                        if (resultSet1.wasNull()) {
                            System.out.println("qq and wechat don't match");
                            return false;
                        }
                    }
                } else if (resultSet.getInt(1) == 0 && (resultSet_qq_wechat.getInt(1) == 0 || (auth.getQq() == null && auth.getWechat() == null))) {
                    System.out.println("qq and wechat are invalid");
                    return false;
                }
            }

            preparedStatement.close();
            preparedStatement1.close();
            preparedStatement2.close();
            delete_statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return true;
    }


    @Override
    public boolean follow(AuthInfo auth, long followeeMid) {
        boolean is_follow = false;
        if (!CheckAuthoInfo(auth, followeeMid)) {
            System.out.println("Authentic information is wrong!");
            return false;
        }
        String sql_check_following = "Select count(*) from user_following where User_1 = ? and User_2 = ?;";

        try (Connection conn_follow_state = dataSource.getConnection();
             PreparedStatement stmt_follow_state = conn_follow_state.prepareStatement(sql_check_following);
        ) {
            stmt_follow_state.setLong(1, auth.getMid());
            stmt_follow_state.setLong(2, followeeMid);
            log.info("SQL: {}", stmt_follow_state);
            ResultSet resultSet_follow_state = stmt_follow_state.executeQuery();
            resultSet_follow_state.next();
            if (resultSet_follow_state.getInt(1) == 0) {
                is_follow = false;
                System.out.println("Not follow yet, set to follow state .....");
                //建立关注关系，需要插入两个表
                String sql_add_following = "UPDATE \"UserRecord\" set following = array_append(following,?) where mid = ?";
                String sql_add_user_following = "INSERT INTO User_Following (user_1, user_2) VALUES (?,?)";
                try (Connection conn_add_following = dataSource.getConnection();
                     Connection conn_add_user_following = dataSource.getConnection();
                     PreparedStatement stmt_add_following = conn_add_following.prepareStatement(sql_add_following);
                     PreparedStatement stmt_add_user_following = conn_add_user_following.prepareStatement(sql_add_user_following);
                ) {
                    stmt_add_following.setLong(1, followeeMid);
                    stmt_add_following.setLong(2, auth.getMid());
                    stmt_add_user_following.setLong(1, auth.getMid());
                    stmt_add_user_following.setLong(2, followeeMid);
                    log.info("SQL: {}", stmt_add_following);
                    log.info("SQL: {}", stmt_add_user_following);
                    stmt_add_following.executeUpdate();
                    stmt_add_user_following.executeUpdate();
                    is_follow = true;
                }
            } else {
                is_follow = true;
                System.out.println("Already follow, now to delete followee  ....");
                //删除关注的记录
                String sql_delete_following = "UPDATE \"UserRecord\" set following = array_remove(following,?) where mid = ?";
                String sql_delete_user_following = "DELETE from user_following where User_1 = ? and User_2 = ?";
                try (Connection conn_delete_following = dataSource.getConnection();
                     Connection conn_delete_user_following = dataSource.getConnection();
                     PreparedStatement stmt_delete_following = conn_delete_following.prepareStatement(sql_delete_following);
                     PreparedStatement stmt_delete_user_following = conn_delete_user_following.prepareStatement(sql_delete_user_following);
                ) {
                    stmt_delete_following.setLong(1, followeeMid);
                    stmt_delete_following.setLong(2, auth.getMid());
                    stmt_delete_user_following.setLong(1, auth.getMid());
                    stmt_delete_user_following.setLong(2, followeeMid);
                    log.info("SQL: {}", stmt_delete_following);
                    log.info("SQL: {}", stmt_delete_user_following);
                    stmt_delete_following.executeUpdate();
                    stmt_delete_user_following.executeUpdate();
                    is_follow = false;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return is_follow;


    }

    @Override
    public UserInfoResp getUserInfo(long mid) {
        String sql_find_user = "Select mid, coin,Following from \"UserRecord\" where mid = ?";
        String sql_find_followee = """
                Select distinct User_1
                from user_following
                where User_2 = ?""";
        String sql_find_watcher = "Select  distinct bv from view_video where View_Mid = ?";
        String sql_find_videolike = "Select distinct Bv from video_like where Video_LIKE_Mid = ?";
        String sql_find_videocollecter = "Select distinct bv  from video_collect where Collected_Mid = ?";
        String sql_find_own = "Select Bv from \"VideoRecord\" where OwnerMid = ?";
        UserInfoResp user = new UserInfoResp();
        try (Connection conn_find_user = dataSource.getConnection();
             Connection conn_find_followee = dataSource.getConnection();
             Connection conn_find_watcher = dataSource.getConnection();
             Connection conn_find_videolike = dataSource.getConnection();
             Connection conn_find_videocollecter = dataSource.getConnection();
             Connection conn_find_own = dataSource.getConnection();
             PreparedStatement stmt_find_user = conn_find_user.prepareStatement(sql_find_user);
             PreparedStatement stmt_find_followee = conn_find_followee.prepareStatement(sql_find_followee);
             PreparedStatement stmt_find_watcher = conn_find_watcher.prepareStatement(sql_find_watcher);
             PreparedStatement stmt_find_videolike = conn_find_videolike.prepareStatement(sql_find_videolike);
             PreparedStatement stmt_find_videocollecter = conn_find_videocollecter.prepareStatement(sql_find_videocollecter);
             PreparedStatement stmt_find_own = conn_find_own.prepareStatement(sql_find_own);
        ) {
            stmt_find_user.setLong(1, mid);
            log.info("SQL: {}", stmt_find_user);
            ResultSet resultSet_find_user = stmt_find_user.executeQuery();
            resultSet_find_user.next();
            if (resultSet_find_user.wasNull()) {
                return null;
            } else {

                user.setMid(mid);
                user.setCoin(resultSet_find_user.getInt(2));
                Array followingArray = resultSet_find_user.getArray(3);
                if (followingArray != null) {
                    Long[] followingData = (Long[]) followingArray.getArray();
                    long[] followingIds = new long[followingData.length];
                    for (int i = 0; i < followingData.length; i++) {
                        followingIds[i] = followingData[i];
                    }
                    user.setFollowing(followingIds);
                }
                stmt_find_followee.setLong(1, mid);
                log.info("SQL: {}", stmt_find_followee);
                ResultSet rs_find_followee = stmt_find_followee.executeQuery();
                List<Long> followeeIds = new ArrayList<>();
                while (rs_find_followee.next()) {
                    followeeIds.add(rs_find_followee.getLong(1));
                }
                long[] followee = new long[followeeIds.size()];
                for (Long followeeId : followeeIds) {
                    followee[followeeIds.indexOf(followeeId)] = followeeId;
                }
                user.setFollower(followee);
                stmt_find_watcher.setLong(1, mid);
                log.info("SQL: {}", stmt_find_watcher);
                ResultSet rs_find_watcher = stmt_find_watcher.executeQuery();
                List<String> videowatcher = new ArrayList<>();
                while (rs_find_watcher.next()) {
                    videowatcher.add(resultSet_find_user.getString(1));
                }
                String[] video_watcher = videowatcher.toArray(new String[0]);
                user.setWatched(video_watcher);
                stmt_find_videolike.setLong(1, mid);
                log.info("SQL: {}", stmt_find_videolike);
                ResultSet rs_find_videolike = stmt_find_videolike.executeQuery();
                List<String> videoliker = new ArrayList<>();
                while (rs_find_videolike.next()) {
                    videoliker.add(rs_find_videolike.getString(1));
                }
                String[] videolike = videoliker.toArray(new String[0]);
                user.setLiked(videolike);
                stmt_find_videocollecter.setLong(1, mid);
                log.info("SQL: {}", stmt_find_videocollecter);
                ResultSet rs_find_videocollecter = stmt_find_videocollecter.executeQuery();
                List<String> videocollector = new ArrayList<>();
                while (rs_find_videocollecter.next()) {
                    videocollector.add(rs_find_videocollecter.getString(1));
                }
                String[] videocollect = videocollector.toArray(new String[0]);
                user.setCollected(videocollect);
                stmt_find_own.setLong(1, mid);
                log.info("SQL: {}", stmt_find_own);
                ResultSet rs_find_own = stmt_find_own.executeQuery();
                rs_find_own.next();
                if (rs_find_own.wasNull()) {
                    user.setPosted(null);
                } else {
                    List<String> posted = new ArrayList<>();
                    while (rs_find_own.next()) {
                        posted.add(rs_find_own.getString(1));
                    }
                    String[] post = posted.toArray(new String[0]);
                    user.setPosted(post);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return user;
    }
}
