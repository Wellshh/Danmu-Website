package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.dto.VideoRecord;
import io.sustc.service.VideoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

@Service
@Slf4j
public class VideoServiceImpl implements VideoService {
    @Autowired
    private DataSource dataSource;

    public boolean CheckAuthoInfo(AuthInfo auth) {
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
            preparedStatement2.setString(1, auth.getQq());
            preparedStatement2.setString(2, auth.getWechat());
            log.info("SQL: {}", preparedStatement);
            log.info("SQL: {}", preparedStatement1);
            log.info("SQL: {}", preparedStatement2);

            try (
                    ResultSet resultSet_qq_wechat = preparedStatement2.executeQuery();
            ) {
                resultSet_qq_wechat.next();
                if (auth.getQq() != null && auth.getWechat() != null) {
                    preparedStatement1.setString(1, auth.getQq());
                    preparedStatement1.setString(2, auth.getWechat());

                    try (ResultSet resultSet1 = preparedStatement1.executeQuery()) {
                        resultSet1.next();
                        if (resultSet1.wasNull()) {
                            System.out.println("qq and wechat don't match");
                            return false;
                        }
                    }
                } else if ((resultSet_qq_wechat.getInt(1) == 0 || (auth.getQq() == null && auth.getWechat() == null))) {
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

    //如何生成独一无二的bv号？
    @Override
    public String postVideo(AuthInfo auth, PostVideoReq req) {
        String sql_sametitle_and_user = "SELECT COUNT(*) FROM \"VideoRecord\" v JOIN \"UserRecord\" u ON v.OwnerMid = u.mid WHERE v.Title = ? AND v.OwnerMid = ?";
        String sql_findmidname = "SELECT u.name FROM \"UserRecord\" u WHERE u.mid = ?";
        VideoRecord newvideoRecord = new VideoRecord();
        if (!CheckAuthoInfo(auth)) {
            System.out.println("Authinfo is wrong!");
            return null;
        } else {
            if (req.getTitle() == null) {
                System.out.println("title is null!");
                return null;
            }
            if (req.getDuration() < 10) {
                System.out.println("duration is too short!");
                return null;
            }
            LocalDateTime localDateTime = LocalDateTime.now();
            if (req.getPublicTime().before(Timestamp.valueOf(localDateTime))) {
                System.out.println("Public time is too early!");
                return null;
            }
            String sql_insertvideo = "Insert into \"VideoRecord\" (bv, title, ownermid, ownername, committime, duration, description)\n" +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";
            try (Connection conn = dataSource.getConnection();
                 Connection conn_insertvideo = dataSource.getConnection();
                 PreparedStatement stmt_sametitle_and_user = conn.prepareStatement(sql_sametitle_and_user);
                 PreparedStatement stmt_findmidname = conn.prepareStatement(sql_findmidname);
                 PreparedStatement stmt_insertuser = conn_insertvideo.prepareStatement(sql_insertvideo);

            ) {
                stmt_sametitle_and_user.setString(1, req.getTitle());
                stmt_sametitle_and_user.setLong(2, auth.getMid());
                log.info("SQL: {}", stmt_sametitle_and_user);
                ResultSet rs_sametitle_and_user = stmt_sametitle_and_user.executeQuery();
                rs_sametitle_and_user.next();
                if (rs_sametitle_and_user.getInt(1) != 0) {
                    System.out.println("There is already a same video!");
                    return null;
                }
                stmt_findmidname.setLong(1, auth.getMid());
                log.info("SQL: {}", stmt_findmidname);
                ResultSet rs_findmidname = stmt_findmidname.executeQuery();
                rs_findmidname.next();
                String authname = rs_findmidname.getString(1);
                newvideoRecord.setBv(generatebv());
                newvideoRecord.setTitle(req.getTitle());
                newvideoRecord.setOwnerMid(auth.getMid());
                newvideoRecord.setOwnerName(authname);
                newvideoRecord.setCommitTime(Timestamp.valueOf(localDateTime));
                newvideoRecord.setDuration(req.getDuration());
                newvideoRecord.setDescription(req.getDescription());
                newvideoRecord.setVideo_is_Deleted(false);
                stmt_insertuser.setString(1, newvideoRecord.getBv());
                stmt_insertuser.setString(2, newvideoRecord.getTitle());
                stmt_sametitle_and_user.setLong(3, newvideoRecord.getOwnerMid());
                stmt_sametitle_and_user.setString(4, newvideoRecord.getOwnerName());
                stmt_sametitle_and_user.setTimestamp(5, newvideoRecord.getCommitTime());
                stmt_sametitle_and_user.setFloat(6, newvideoRecord.getDuration());
                stmt_sametitle_and_user.setString(7, newvideoRecord.getDescription());
                log.info("SQL: {}", stmt_insertuser);
                stmt_sametitle_and_user.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return newvideoRecord.getBv();
    }

    public String generatebv() {
        return "BV2".concat(String.valueOf(System.currentTimeMillis()));
    }

    @Override
    public boolean deleteVideo(AuthInfo auth, String bv) {
        return true;
    }

    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        return false;
    }

    @Override
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        return null;
    }

    @Override
    public double getAverageViewRate(String bv) {
        return 0;
    }

    @Override
    public Set<Integer> getHotspot(String bv) {
        return null;
    }

    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        return false;
    }
}
